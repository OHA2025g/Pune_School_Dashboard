import { useState, useEffect, useCallback } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import { useScope } from "@/context/ScopeContext";
import KPICard from "../components/KPICard";
import DataTable from "../components/DataTable";
import RAGBadge from "../components/RAGBadge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { 
  Trophy, 
  TrendingUp,
  TrendingDown,
  Target,
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
  Cell,
  PieChart,
  Pie
} from "recharts";

import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;

const SchoolHealthIndex = () => {
  const { scope } = useScope();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [data, setData] = useState(null);
  const [topDistricts, setTopDistricts] = useState([]);
  const [bottomDistricts, setBottomDistricts] = useState([]);

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [shiRes, topRes, bottomRes] = await Promise.all([
        axios.get(`${API}/analytics/shi-distribution`),
        axios.get(`${API}/rankings/districts/top?limit=10`),
        axios.get(`${API}/rankings/districts/bottom?limit=10`)
      ]);
      setData(shiRes.data);
      setTopDistricts(topRes.data);
      setBottomDistricts(bottomRes.data);
    } catch (error) {
      console.error("Error fetching SHI data:", error);
    } finally {
      setLoading(false);
    }
  }, [scope.version]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="loading-spinner" />
      </div>
    );
  }

  const tableColumns = [
    { key: "district_name", label: "District", sortable: true },
    { key: "shi_score", label: "SHI Score", type: "progress", sortable: true },
    { key: "rag_status", label: "Status", type: "rag" },
  ];

  const distributionData = [
    { name: "Excellent (≥85)", value: data?.distribution?.excellent || 0, color: "#10b981" },
    { name: "Good (70-84)", value: data?.distribution?.good || 0, color: "#3b82f6" },
    { name: "At Risk (50-69)", value: data?.distribution?.at_risk || 0, color: "#f59e0b" },
    { name: "Critical (<50)", value: data?.distribution?.critical || 0, color: "#ef4444" },
  ];

  return (
    <div className="space-y-6 animate-fade-in" data-testid="school-health-index">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight" style={{ fontFamily: 'Manrope' }}>
            School Health Index (SHI)
          </h1>
          <p className="text-slate-500 mt-1">Composite performance score across all parameters</p>
        </div>
        <Button variant="outline" size="sm" onClick={fetchData}>
          <RefreshCw className="w-4 h-4 mr-2" />
          Refresh
        </Button>
      </div>

      {/* KPI Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <KPICard
          label="State Average SHI"
          value={data?.avg_shi}
          icon={Trophy}
          trend="up"
          trendValue="+2.1 this quarter"
          testId="kpi-avg-shi"
        />
        <KPICard
          label="Highest SHI"
          value={data?.max_shi}
          icon={TrendingUp}
          className="border-l-4 border-l-emerald-500"
          testId="kpi-max-shi"
        />
        <KPICard
          label="Lowest SHI"
          value={data?.min_shi}
          icon={TrendingDown}
          className="border-l-4 border-l-red-500"
          testId="kpi-min-shi"
        />
        <KPICard
          label="Target SHI"
          value={85}
          icon={Target}
          testId="kpi-target-shi"
        />
      </div>

      {/* SHI Formula Info */}
      <Card className="border-slate-200 bg-gradient-to-br from-slate-900 to-slate-800 text-white">
        <CardContent className="p-6">
          <h3 className="text-lg font-semibold mb-4" style={{ fontFamily: 'Manrope' }}>
            SHI Formula Components
          </h3>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
            {[
              { label: "Identity Score", weight: "25%", desc: "Aadhaar + APAAR" },
              { label: "Infrastructure", weight: "25%", desc: "Water + Toilet + Classroom" },
              { label: "Teacher Score", weight: "20%", desc: "PTR Compliance" },
              { label: "Operational", weight: "20%", desc: "Data Entry Status" },
              { label: "Age Integrity", weight: "10%", desc: "Age-Class Match" },
            ].map((item, idx) => (
              <div key={idx} className="text-center p-3 bg-white/10 rounded-lg">
                <p className="text-2xl font-bold text-white">{item.weight}</p>
                <p className="text-sm text-slate-300 mt-1">{item.label}</p>
                <p className="text-xs text-slate-400 mt-1">{item.desc}</p>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* Distribution Chart */}
        <Card className="border-slate-200">
          <CardHeader>
            <CardTitle style={{ fontFamily: 'Manrope' }}>SHI Distribution</CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={distributionData}
                    cx="50%"
                    cy="50%"
                    innerRadius={50}
                    outerRadius={80}
                    paddingAngle={2}
                    dataKey="value"
                  >
                    {distributionData.map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: 'rgba(15, 23, 42, 0.95)',
                      border: 'none',
                      borderRadius: '8px',
                      color: 'white'
                    }}
                  />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div className="grid grid-cols-2 gap-2 mt-4">
              {distributionData.map((item) => (
                <div key={item.name} className="flex items-center gap-2 text-sm">
                  <div 
                    className="w-3 h-3 rounded-full" 
                    style={{ backgroundColor: item.color }}
                  />
                  <span className="text-slate-600">{item.name}: <strong>{item.value}</strong></span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        {/* Top Districts */}
        <Card className="border-slate-200">
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
              <TrendingUp className="w-5 h-5 text-emerald-500" />
              Top 10 Districts
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2 max-h-80 overflow-y-auto">
              {topDistricts.map((district, idx) => (
                <div 
                  key={district.district_code}
                  className="flex items-center justify-between p-3 bg-slate-50 rounded-lg cursor-pointer hover:bg-slate-100 transition-colors"
                  onClick={() => navigate(`/district/${district.district_code}`)}
                  data-testid={`top-district-${idx}`}
                >
                  <div className="flex items-center gap-3">
                    <span className="w-6 h-6 flex items-center justify-center bg-emerald-100 text-emerald-700 text-xs font-bold rounded-full">
                      {idx + 1}
                    </span>
                    <span className="font-medium text-slate-900 text-sm">{district.district_name}</span>
                  </div>
                  <span className="font-bold text-emerald-600 tabular-nums">{district.shi_score}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        {/* Bottom Districts */}
        <Card className="border-slate-200">
          <CardHeader className="pb-2">
            <CardTitle className="flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
              <TrendingDown className="w-5 h-5 text-red-500" />
              Bottom 10 Districts
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-2 max-h-80 overflow-y-auto">
              {bottomDistricts.map((district, idx) => (
                <div 
                  key={district.district_code}
                  className="flex items-center justify-between p-3 bg-slate-50 rounded-lg cursor-pointer hover:bg-slate-100 transition-colors"
                  onClick={() => navigate(`/district/${district.district_code}`)}
                  data-testid={`bottom-district-${idx}`}
                >
                  <div className="flex items-center gap-3">
                    <span className="w-6 h-6 flex items-center justify-center bg-red-100 text-red-700 text-xs font-bold rounded-full">
                      {idx + 1}
                    </span>
                    <span className="font-medium text-slate-900 text-sm">{district.district_name}</span>
                  </div>
                  <span className="font-bold text-red-600 tabular-nums">{district.shi_score}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* All Districts Table */}
      <Card className="border-slate-200">
        <CardHeader>
          <CardTitle style={{ fontFamily: 'Manrope' }}>All Districts - SHI Rankings</CardTitle>
        </CardHeader>
        <CardContent>
          <DataTable 
            data={data?.district_scores || []}
            columns={tableColumns}
            onRowClick={(row) => {
              const district = topDistricts.find(d => d.district_name === row.district_name) ||
                              bottomDistricts.find(d => d.district_name === row.district_name);
              if (district) navigate(`/district/${district.district_code}`);
            }}
            testId="shi-table"
          />
        </CardContent>
      </Card>

      {/* SHI Classification */}
      <Card className="border-slate-200">
        <CardHeader>
          <CardTitle style={{ fontFamily: 'Manrope' }}>SHI Classification Guide</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            {[
              { range: "≥ 85", label: "Excellent", action: "Best Practice Sharing", color: "emerald" },
              { range: "70 - 84", label: "Good", action: "Regular Monitoring", color: "blue" },
              { range: "50 - 69", label: "At Risk", action: "Intervention Required", color: "amber" },
              { range: "< 50", label: "Critical", action: "Immediate Action", color: "red" },
            ].map((item) => (
              <div 
                key={item.label}
                className={`p-4 rounded-lg border-l-4 bg-${item.color}-50 border-${item.color}-500`}
                style={{ 
                  backgroundColor: item.color === 'emerald' ? '#ecfdf5' : 
                                   item.color === 'blue' ? '#eff6ff' :
                                   item.color === 'amber' ? '#fffbeb' : '#fef2f2',
                  borderLeftColor: item.color === 'emerald' ? '#10b981' : 
                                   item.color === 'blue' ? '#3b82f6' :
                                   item.color === 'amber' ? '#f59e0b' : '#ef4444'
                }}
              >
                <p className="text-2xl font-bold text-slate-900">{item.range}</p>
                <p className="font-semibold text-slate-700 mt-1">{item.label}</p>
                <p className="text-sm text-slate-500 mt-1">{item.action}</p>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>
    </div>
  );
};

export default SchoolHealthIndex;
