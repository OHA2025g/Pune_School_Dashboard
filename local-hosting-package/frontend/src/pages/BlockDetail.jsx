import { useState, useEffect } from "react";
import { useParams, useNavigate } from "react-router-dom";
import axios from "axios";
import KPICard from "../components/KPICard";
import DataTable from "../components/DataTable";
import RAGBadge from "../components/RAGBadge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { 
  ArrowLeft,
  School, 
  Users, 
  ShieldCheck, 
  Trophy,
  MapPin,
  Building2,
  RefreshCw,
  Filter,
  Brain,
  Loader2
} from "lucide-react";
import { toast } from "sonner";
import AiInsightsCard from "@/components/AiInsightsCard";

import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;

const BlockDetail = () => {
  const { blockCode } = useParams();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [block, setBlock] = useState(null);
  const [schools, setSchools] = useState([]);
  const [ragFilter, setRagFilter] = useState("all");
  const [insightsLoading, setInsightsLoading] = useState(false);
  const [insights, setInsights] = useState("");

  const fetchData = async () => {
    setLoading(true);
    try {
      const [blockRes, schoolsRes] = await Promise.all([
        axios.get(`${API}/blocks/${blockCode}`),
        axios.get(`${API}/blocks/${blockCode}/schools?limit=100${ragFilter !== 'all' ? `&rag_filter=${ragFilter}` : ''}`)
      ]);
      setBlock(blockRes.data);
      setSchools(schoolsRes.data);
    } catch (error) {
      console.error("Error fetching block data:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [blockCode, ragFilter]);

  const generateInsights = async () => {
    setInsightsLoading(true);
    try {
      const res = await axios.get(`${API}/analytics/insights/executive-summary`, {
        params: { block_code: blockCode },
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
    if (insights) await generateInsights();
  };

  const schoolColumns = [
    { key: "school_name", label: "School Name", sortable: true },
    { key: "school_category", label: "Category" },
    { key: "total_students", label: "Students", type: "number", sortable: true },
    { key: "total_teachers", label: "Teachers", type: "number" },
    { key: "ptr", label: "PTR", type: "number", sortable: true },
    { key: "aadhaar_percentage", label: "Aadhaar %", type: "percentage", sortable: true },
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

  if (!block) {
    return (
      <div className="text-center py-12">
        <p className="text-slate-500">Block not found</p>
        <Button variant="outline" onClick={() => navigate(-1)} className="mt-4">
          Go Back
        </Button>
      </div>
    );
  }

  return (
    <div className="space-y-6 animate-fade-in" data-testid="block-detail">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div className="flex items-start gap-4">
          <Button 
            variant="ghost" 
            size="icon" 
            onClick={() => navigate(`/district/${block.district_code}`)}
            data-testid="back-btn"
          >
            <ArrowLeft className="w-5 h-5" />
          </Button>
          <div>
            <div className="flex items-center gap-3 mb-2">
              <h1 className="text-3xl font-bold text-slate-900 tracking-tight" style={{ fontFamily: 'Manrope' }}>
                {block.block_name}
              </h1>
              <RAGBadge status={block.rag_status} size="large" />
            </div>
            <div className="flex items-center gap-4 text-slate-500">
              <span className="flex items-center gap-1">
                <MapPin className="w-4 h-4" />
                {block.district_name}
              </span>
              <span className="flex items-center gap-1">
                <Building2 className="w-4 h-4" />
                Block Code: {block.block_code}
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
          value={block.total_schools}
          icon={School}
          testId="block-kpi-schools"
        />
        <KPICard
          label="Total Students"
          value={block.total_students}
          icon={Users}
          testId="block-kpi-students"
        />
        <KPICard
          label="Aadhaar Compliance"
          value={block.aadhaar_percentage}
          suffix="%"
          icon={ShieldCheck}
          testId="block-kpi-aadhaar"
        />
        <KPICard
          label="SHI Score"
          value={block.shi_score}
          icon={Trophy}
          testId="block-kpi-shi"
        />
      </div>

      {/* AI Insights */}
      {!insights ? (
        <Card className="border-slate-200">
          <CardHeader>
            <CardTitle style={{ fontFamily: "Manrope" }}>AI Insights (Block)</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
            <div className="text-slate-600">
              Generate scoped insights for this block: Insights, Root Cause Signals, Recommendations, Priority Action Items.
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
        <AiInsightsCard title="AI Insights (Block)" content={insights} loading={insightsLoading} />
      )}

      {/* Schools Table */}
      <Card className="border-slate-200">
        <CardHeader>
          <div className="flex items-center justify-between">
            <CardTitle style={{ fontFamily: 'Manrope' }}>Schools ({schools.length})</CardTitle>
            <div className="flex items-center gap-3">
              <Filter className="w-4 h-4 text-slate-400" />
              <Select value={ragFilter} onValueChange={setRagFilter}>
                <SelectTrigger className="w-32" data-testid="rag-filter">
                  <SelectValue placeholder="Filter" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="all">All</SelectItem>
                  <SelectItem value="green">Excellent</SelectItem>
                  <SelectItem value="amber">At Risk</SelectItem>
                  <SelectItem value="red">Critical</SelectItem>
                </SelectContent>
              </Select>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          <DataTable 
            data={schools}
            columns={schoolColumns}
            onRowClick={(row) => navigate(`/school/${row.udise_code}`)}
            testId="schools-table"
          />
        </CardContent>
      </Card>
    </div>
  );
};

export default BlockDetail;
