import { useState, useEffect } from "react";
import { useParams, useNavigate } from "react-router-dom";
import axios from "axios";
import KPICard from "../components/KPICard";
import DataTable from "../components/DataTable";
import RAGBadge from "../components/RAGBadge";
import ProgressBar from "../components/ProgressBar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { 
  ArrowLeft,
  School, 
  Users, 
  ShieldCheck, 
  Trophy,
  MapPin,
  Building2,
  RefreshCw
} from "lucide-react";
import {
  BarChart,
  Bar,
  XAxis,
  YAxis,
  CartesianGrid,
  Tooltip,
  ResponsiveContainer,
  Cell
} from "recharts";
import { Brain, Loader2 } from "lucide-react";
import { toast } from "sonner";
import AiInsightsCard from "@/components/AiInsightsCard";

import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;

const DistrictDetail = () => {
  const { districtCode } = useParams();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [district, setDistrict] = useState(null);
  const [blocks, setBlocks] = useState([]);
  const [insightsLoading, setInsightsLoading] = useState(false);
  const [insights, setInsights] = useState("");

  const fetchData = async () => {
    setLoading(true);
    try {
      const [districtRes, blocksRes] = await Promise.all([
        axios.get(`${API}/districts/${districtCode}`),
        axios.get(`${API}/districts/${districtCode}/blocks`)
      ]);
      setDistrict(districtRes.data);
      setBlocks(blocksRes.data);
    } catch (error) {
      console.error("Error fetching district data:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [districtCode]);

  const generateInsights = async () => {
    setInsightsLoading(true);
    try {
      const res = await axios.get(`${API}/analytics/insights/executive-summary`, {
        // Explicitly set scope for this view (prevents stale global scope from affecting results)
        params: { district_code: districtCode },
      });
      setInsights(res.data?.ai_summary || "");
      toast.success("Insights generated");
    } catch (error) {
      const status = error?.response?.status;
      const detail = error?.response?.data?.detail || error?.message || "Failed to generate insights";
      if (status === 401) navigate("/login");
      toast.error(detail);
    } finally {
      setInsightsLoading(false);
    }
  };

  const refreshAll = async () => {
    await fetchData();
    // If insights were already generated, regenerate so they reflect refreshed data.
    if (insights) await generateInsights();
  };

  const blockColumns = [
    { key: "block_name", label: "Block Name", sortable: true },
    { key: "total_schools", label: "Schools", type: "number", sortable: true },
    { key: "total_students", label: "Students", type: "number", sortable: true },
    { key: "aadhaar_percentage", label: "Aadhaar %", type: "percentage", sortable: true },
    { key: "apaar_percentage", label: "APAAR %", type: "percentage", sortable: true },
    { key: "shi_score", label: "SHI Score", type: "progress", sortable: true },
    { key: "rag_status", label: "Status", type: "rag" },
  ];

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="loading-spinner" />
      </div>
    );
  }

  if (!district) {
    return (
      <div className="text-center py-12">
        <p className="text-slate-500">District not found</p>
        <Button variant="outline" onClick={() => navigate("/")} className="mt-4">
          Go Back
        </Button>
      </div>
    );
  }

  return (
    <div className="space-y-6 animate-fade-in" data-testid="district-detail">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div className="flex items-start gap-4">
          <Button 
            variant="ghost" 
            size="icon" 
            onClick={() => navigate("/")}
            data-testid="back-btn"
          >
            <ArrowLeft className="w-5 h-5" />
          </Button>
          <div>
            <div className="flex items-center gap-3 mb-2">
              <h1 className="text-3xl font-bold text-slate-900 tracking-tight" style={{ fontFamily: 'Manrope' }}>
                {district.district_name}
              </h1>
              <RAGBadge status={district.rag_status} size="large" />
            </div>
            <div className="flex items-center gap-4 text-slate-500">
              <span className="flex items-center gap-1">
                <MapPin className="w-4 h-4" />
                District Code: {district.district_code}
              </span>
              <span className="flex items-center gap-1">
                <Building2 className="w-4 h-4" />
                {blocks.length} Blocks
              </span>
            </div>
          </div>
        </div>
        <Button variant="outline" size="sm" onClick={refreshAll}>
          <RefreshCw className="w-4 h-4 mr-2" />
          Refresh
        </Button>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <KPICard
          label="Total Schools"
          value={district.total_schools}
          icon={School}
          testId="district-kpi-schools"
        />
        <KPICard
          label="Total Students"
          value={district.total_students}
          icon={Users}
          testId="district-kpi-students"
        />
        <KPICard
          label="Aadhaar Compliance"
          value={district.aadhaar_percentage}
          suffix="%"
          icon={ShieldCheck}
          testId="district-kpi-aadhaar"
        />
        <KPICard
          label="SHI Score"
          value={district.shi_score}
          icon={Trophy}
          testId="district-kpi-shi"
        />
      </div>

      {/* AI Insights */}
      {!insights ? (
        <Card className="border-slate-200">
          <CardHeader>
            <CardTitle style={{ fontFamily: "Manrope" }}>AI Insights (District)</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
            <div className="text-slate-600">
              Generate scoped insights for this district: Insights, Root Cause Signals, Recommendations, Priority Action Items.
            </div>
            <Button onClick={generateInsights} disabled={insightsLoading}>
              {insightsLoading ? (
                <>
                  <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                  Generating…
                </>
              ) : (
                <>
                  <Brain className="w-4 h-4 mr-2" />
                  Generate Insights
                </>
              )}
            </Button>
          </CardContent>
        </Card>
      ) : (
        <AiInsightsCard title="AI Insights (District)" content={insights} loading={insightsLoading} />
      )}

      {/* Block Performance Chart */}
      <Card className="border-slate-200">
        <CardHeader>
          <CardTitle style={{ fontFamily: 'Manrope' }}>Block-wise SHI Performance</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="h-80">
            <ResponsiveContainer width="100%" height="100%">
              <BarChart data={blocks} layout="vertical" margin={{ left: 100 }}>
                <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                <XAxis type="number" domain={[0, 100]} />
                <YAxis dataKey="block_name" type="category" width={90} />
                <Tooltip 
                  contentStyle={{ 
                    backgroundColor: 'rgba(15, 23, 42, 0.95)',
                    border: 'none',
                    borderRadius: '8px',
                    color: 'white'
                  }}
                />
                <Bar dataKey="shi_score" radius={[0, 4, 4, 0]}>
                  {blocks.map((entry, index) => (
                    <Cell 
                      key={`cell-${index}`} 
                      fill={
                        entry.rag_status === "green" ? "#10b981" :
                        entry.rag_status === "amber" ? "#f59e0b" : "#ef4444"
                      } 
                    />
                  ))}
                </Bar>
              </BarChart>
            </ResponsiveContainer>
          </div>
        </CardContent>
      </Card>

      {/* Blocks Table */}
      <Card className="border-slate-200">
        <CardHeader>
          <CardTitle style={{ fontFamily: 'Manrope' }}>All Blocks ({blocks.length})</CardTitle>
        </CardHeader>
        <CardContent>
          <DataTable 
            data={blocks}
            columns={blockColumns}
            onRowClick={(row) => navigate(`/block/${row.block_code}`)}
            testId="blocks-table"
          />
        </CardContent>
      </Card>
    </div>
  );
};

export default DistrictDetail;
