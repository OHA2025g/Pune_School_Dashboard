import { useState, useEffect, useCallback } from "react";
import axios from "axios";
import { useScope } from "@/context/ScopeContext";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { 
  Users, 
  GraduationCap,
  AlertTriangle, 
  RefreshCw,
  Upload,
  School,
  TrendingUp,
  TrendingDown,
  UserCheck,
  BarChart3,
  Target,
  Activity,
  Brain
} from "lucide-react";
import { toast } from "sonner";
import ExportPanel from "@/components/ExportPanel";
import AiInsightsTab from "@/components/AiInsightsTab";
import { BlockLink, SchoolLink } from "@/components/DrilldownLink";
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
  Pie,
  Legend,
  LineChart,
  Line,
  ComposedChart,
  Area
} from "recharts";

import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;

// KPI Card Component
const KPICard = ({ label, value, suffix = "", icon: Icon, color = "blue", description, trend, trendValue }) => {
  const colors = {
    blue: "bg-blue-50 text-blue-600",
    green: "bg-emerald-50 text-emerald-600",
    red: "bg-red-50 text-red-600",
    amber: "bg-amber-50 text-amber-600",
    purple: "bg-purple-50 text-purple-600",
    cyan: "bg-cyan-50 text-cyan-600",
  };

  return (
    <Card className="border-slate-200" data-testid={`kpi-${label.toLowerCase().replace(/\s+/g, '-')}`}>
      <CardContent className="p-4">
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <p className="text-xs font-medium text-slate-500 uppercase tracking-wider">{label}</p>
            <div className="flex items-baseline gap-1 mt-1">
              <span className="text-2xl font-bold text-slate-900 tabular-nums" style={{ fontFamily: 'Manrope' }}>
                {typeof value === 'number' ? value.toLocaleString('en-IN') : value}
              </span>
              {suffix && <span className="text-lg text-slate-500">{suffix}</span>}
            </div>
            {trend !== undefined && trendValue !== undefined && (
              <div className={`flex items-center gap-1 mt-1 text-sm ${trend === 'up' ? 'text-emerald-600' : trend === 'down' ? 'text-red-600' : 'text-slate-500'}`}>
                {trend === 'up' ? <TrendingUp className="w-3 h-3" /> : <TrendingDown className="w-3 h-3" />}
                <span>{trendValue}</span>
              </div>
            )}
            {description && <p className="text-xs text-slate-400 mt-1">{description}</p>}
          </div>
          {Icon && (
            <div className={`p-2 rounded-lg ${colors[color]}`}>
              <Icon className="w-5 h-5" strokeWidth={1.5} />
            </div>
          )}
        </div>
      </CardContent>
    </Card>
  );
};

// Gauge Component for Retention
const RetentionGauge = ({ value, label, target = 90 }) => {
  const getColor = (val) => {
    if (val >= target) return "#10b981";
    if (val >= target - 10) return "#f59e0b";
    return "#ef4444";
  };
  
  return (
    <div className="text-center">
      <div className="relative w-24 h-24 mx-auto">
        <svg className="w-full h-full transform -rotate-90">
          <circle cx="48" cy="48" r="40" stroke="#e2e8f0" strokeWidth="8" fill="none" />
          <circle 
            cx="48" cy="48" r="40" 
            stroke={getColor(value)} 
            strokeWidth="8" 
            fill="none"
            strokeDasharray={`${(value / 100) * 251.2} 251.2`}
            strokeLinecap="round"
          />
        </svg>
        <div className="absolute inset-0 flex items-center justify-center">
          <span className="text-xl font-bold" style={{ color: getColor(value) }}>{value}%</span>
        </div>
      </div>
      <p className="text-sm text-slate-600 mt-2">{label}</p>
    </div>
  );
};

const EnrolmentDashboard = () => {
  const { scope } = useScope();
  const [loading, setLoading] = useState(true);
  const [importing, setImporting] = useState(false);
  const [overview, setOverview] = useState(null);
  const [classData, setClassData] = useState([]);
  const [stageData, setStageData] = useState([]);
  const [sizeDistribution, setSizeDistribution] = useState([]);
  const [blockData, setBlockData] = useState([]);
  const [retentionData, setRetentionData] = useState([]);
  const [smallSchools, setSmallSchools] = useState([]);
  const [largeSchools, setLargeSchools] = useState([]);
  const [activeTab, setActiveTab] = useState("executive");

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [
        overviewRes,
        classRes,
        stageRes,
        sizeRes,
        blockRes,
        retentionRes,
        smallRes,
        largeRes
      ] = await Promise.all([
        axios.get(`${API}/enrolment/overview`),
        axios.get(`${API}/enrolment/class-wise`),
        axios.get(`${API}/enrolment/stage-wise`),
        axios.get(`${API}/enrolment/school-size-distribution`),
        axios.get(`${API}/enrolment/block-wise`),
        axios.get(`${API}/enrolment/retention-analysis`),
        axios.get(`${API}/enrolment/risk-schools?risk_type=small`),
        axios.get(`${API}/enrolment/risk-schools?risk_type=large`)
      ]);
      
      setOverview(overviewRes.data);
      setClassData(classRes.data);
      setStageData(stageRes.data);
      setSizeDistribution(sizeRes.data);
      setBlockData(blockRes.data);
      setRetentionData(retentionRes.data);
      setSmallSchools(smallRes.data);
      setLargeSchools(largeRes.data);
    } catch (error) {
      console.error("Error fetching Enrolment data:", error);
      toast.error("Failed to load enrolment data");
    } finally {
      setLoading(false);
    }
  }, [scope.version]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handleImport = async () => {
    const url = "https://customer-assets.emergentagent.com/job_7d6cbc1e-b567-4fbc-af84-06fa5107bbd4/artifacts/l66i3550_4.%20Enrolment_Class_Wise_All_Student%20%20-%202025-26%20-%20%28State%20%29%20MAHARASHTRA.xlsx";
    
    setImporting(true);
    try {
      await axios.post(`${API}/enrolment/import?url=${encodeURIComponent(url)}`);
      toast.success("Enrolment data import started!");
      setTimeout(() => {
        fetchData();
        setImporting(false);
      }, 5000);
    } catch (error) {
      toast.error("Import failed: " + (error.response?.data?.detail || error.message));
      setImporting(false);
    }
  };

  const CustomTooltip = ({ active, payload, label }) => {
    if (active && payload && payload.length) {
      return (
        <div className="bg-slate-900 text-white p-3 rounded-lg shadow-lg">
          <p className="font-medium">{label}</p>
          {payload.map((entry, index) => (
            <p key={index} className="text-sm" style={{ color: entry.color }}>
              {entry.name}: {typeof entry.value === 'number' ? entry.value.toLocaleString() : entry.value}
            </p>
          ))}
        </div>
      );
    }
    return null;
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="loading-spinner" />
      </div>
    );
  }

  const hasData = overview && overview.total_schools > 0;

  const buildInsights = () => {
    if (!overview) {
      return [
        "## Insights",
        "- No enrolment data available for the current filters.",
        "",
        "## Root Cause Signals",
        "- Missing or incomplete records in the selected scope.",
        "",
        "## Recommendations",
        "- Refresh data or broaden filters to include valid records.",
        "",
        "## Priority Action Items",
        "- Validate data import for enrolment analytics.",
      ].join("\n");
    }

    const fmt = (v) => (typeof v === "number" ? v.toLocaleString("en-IN") : v ?? 0);
    const pct = (v) => (typeof v === "number" ? `${v}%` : "0%");
    const blocks = Array.isArray(blockData) ? blockData : [];
    const worstRetention = [...blocks].sort((a, b) => (a.secondary_retention || 0) - (b.secondary_retention || 0)).slice(0, 3);

    return [
      "## Insights",
      `- Total enrolment: **${fmt(overview.grand_total)}** across **${fmt(overview.total_schools)}** schools.`,
      `- Girls participation: **${pct(overview.girls_participation_pct)}** with GPI **${overview.gender_parity_index}**.`,
      `- Secondary retention index: **${overview.secondary_retention_index}**.`,
      worstRetention.length ? `- Lowest retention blocks: **${worstRetention.map((b) => b.block_name).join(", ")}**.` : "- Lowest retention blocks: unavailable.",
      "",
      "## Root Cause Signals",
      "- Lower secondary retention indicates transition gaps from upper primary to secondary.",
      "- GPI below parity in select blocks suggests gender participation barriers.",
      "",
      "## Recommendations",
      "- Focus retention drives in low-retention blocks with school-level tracking.",
      "- Strengthen transition support (counselling, transport, scholarships).",
      "",
      "## Priority Action Items",
      worstRetention.length ? `- Week 1: boost retention in **${worstRetention.map((b) => b.block_name).join(", ")}** where secondary retention is lowest.` : "- Week 1: boost retention in lowest retention blocks.",
      `- Week 2: lift girls participation (**${pct(overview.girls_participation_pct)}**) via targeted enrolment drives.`,
      "- Week 3–4: review transition drop-off and update block action plans.",
    ].join("\n");
  };

  return (
    <div className="space-y-6 animate-fade-in" data-testid="enrolment-dashboard">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight" style={{ fontFamily: 'Manrope' }}>
            Enrolment Analytics Dashboard
          </h1>
          <p className="text-slate-500 mt-1">Class-wise Student Enrolment • AY 2025-26 • Pune District</p>
        </div>
        <div className="flex items-center gap-3">
          <Button 
            variant="outline" 
            size="sm" 
            onClick={handleImport}
            disabled={importing}
            data-testid="import-enrolment-btn"
          >
            {importing ? <RefreshCw className="w-4 h-4 mr-2 animate-spin" /> : <Upload className="w-4 h-4 mr-2" />}
            {importing ? "Importing..." : "Import Data"}
          </Button>
          <ExportPanel dashboardName="enrolment" dashboardTitle="Enrolment Analytics" />
          <Button variant="outline" size="sm" onClick={fetchData} data-testid="refresh-btn">
            <RefreshCw className="w-4 h-4 mr-2" />
            Refresh
          </Button>
        </div>
      </div>

      {!hasData ? (
        <Card className="border-slate-200">
          <CardContent className="py-12 text-center">
            <GraduationCap className="w-16 h-16 mx-auto text-slate-300 mb-4" />
            <h3 className="text-xl font-semibold text-slate-700 mb-2">No Enrolment Data Available</h3>
            <p className="text-slate-500 mb-4">Click "Import Data" to load the Class-wise Enrolment Excel file</p>
            <Button onClick={handleImport} disabled={importing} data-testid="import-enrolment-empty-btn">
              {importing ? "Importing..." : "Import Enrolment Data"}
            </Button>
          </CardContent>
        </Card>
      ) : (
        <>
          {/* Dashboard Tabs */}
          <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
            <TabsList className="bg-slate-100">
              <TabsTrigger value="executive" className="flex items-center gap-2" data-testid="tab-executive">
                <BarChart3 className="w-4 h-4" />
                Executive Overview
              </TabsTrigger>
              <TabsTrigger value="classwise" className="flex items-center gap-2" data-testid="tab-classwise">
                <GraduationCap className="w-4 h-4" />
                Class & Stage
              </TabsTrigger>
              <TabsTrigger value="gender" className="flex items-center gap-2" data-testid="tab-gender">
                <Users className="w-4 h-4" />
                Gender & Inclusion
              </TabsTrigger>
              <TabsTrigger value="retention" className="flex items-center gap-2" data-testid="tab-retention">
                <Activity className="w-4 h-4" />
                Retention & Risk
              </TabsTrigger>
              <TabsTrigger value="insights" className="flex items-center gap-2" data-testid="tab-insights">
                <Brain className="w-4 h-4" />
                Insights
              </TabsTrigger>
            </TabsList>

            {/* DASHBOARD 1: EXECUTIVE OVERVIEW */}
            <TabsContent value="executive" className="space-y-6">
              {/* Top KPI Strip */}
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
                <KPICard
                  label="Total Schools"
                  value={overview.total_schools}
                  icon={School}
                  color="blue"
                />
                <KPICard
                  label="Total Enrolment"
                  value={overview.grand_total}
                  icon={Users}
                  color="purple"
                />
                <KPICard
                  label="Girls %"
                  value={overview.girls_participation_pct}
                  suffix="%"
                  icon={UserCheck}
                  color={overview.girls_participation_pct >= 45 ? "green" : "red"}
                />
                <KPICard
                  label="Gender Parity"
                  value={overview.gender_parity_index}
                  icon={Users}
                  color={overview.gender_parity_index >= 0.95 ? "green" : "amber"}
                  description="GPI (Girls/Boys)"
                />
                <KPICard
                  label="Avg School Size"
                  value={overview.avg_school_size}
                  icon={School}
                  color="cyan"
                />
                <KPICard
                  label="Sec→HS Retention"
                  value={overview.secondary_hs_retention}
                  suffix="%"
                  icon={Activity}
                  color={overview.secondary_hs_retention >= 80 ? "green" : "red"}
                  description="Class 10→11"
                />
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Stage-wise Distribution Donut */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Education Stage Distribution
                    </CardTitle>
                    <CardDescription>NEP-aligned stage grouping</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-72">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={stageData}
                            cx="50%"
                            cy="50%"
                            innerRadius={50}
                            outerRadius={90}
                            paddingAngle={2}
                            dataKey="total"
                            label={({ name, percentage }) => `${percentage}%`}
                          >
                            {stageData.map((entry, index) => (
                              <Cell key={`cell-${index}`} fill={entry.color} />
                            ))}
                          </Pie>
                          <Tooltip content={<CustomTooltip />} />
                          <Legend />
                        </PieChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>

                {/* School Size Distribution */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      School Size Distribution
                    </CardTitle>
                    <CardDescription>Number of schools by student count</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-72">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={sizeDistribution}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis dataKey="range" tick={{ fontSize: 11 }} />
                          <YAxis />
                          <Tooltip content={<CustomTooltip />} />
                          <Bar dataKey="count" name="Schools" radius={[4, 4, 0, 0]}>
                            {sizeDistribution.map((entry, index) => (
                              <Cell key={`cell-${index}`} fill={entry.color} />
                            ))}
                          </Bar>
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Class-wise Enrolment Bar */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Class-wise Enrolment Trend (PP3 to Class 12)
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="h-72">
                    <ResponsiveContainer width="100%" height="100%">
                      <ComposedChart data={classData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="class_name" tick={{ fontSize: 10 }} />
                        <YAxis />
                        <Tooltip content={<CustomTooltip />} />
                        <Legend />
                        <Bar dataKey="total" name="Total Enrolment" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                        <Line type="monotone" dataKey="gpi" name="Gender Parity Index" stroke="#10b981" strokeWidth={2} yAxisId="right" dot={{ r: 3 }} />
                        <YAxis yAxisId="right" orientation="right" domain={[0, 1.5]} />
                      </ComposedChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* DASHBOARD 2: CLASS & STAGE ANALYSIS */}
            <TabsContent value="classwise" className="space-y-6">
              {/* Stage KPIs */}
              <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                <Card className="border-slate-200 border-l-4 border-l-purple-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Pre-Primary</p>
                    <p className="text-2xl font-bold text-purple-600">{overview.pp_total?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">{overview.pp_pct}% share</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-blue-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Primary (1-5)</p>
                    <p className="text-2xl font-bold text-blue-600">{overview.primary_total?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">{overview.primary_pct}% share</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-emerald-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Upper Primary (6-8)</p>
                    <p className="text-2xl font-bold text-emerald-600">{overview.upper_primary_total?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">{overview.upper_primary_pct}% share</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-amber-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Secondary (9-10)</p>
                    <p className="text-2xl font-bold text-amber-600">{overview.secondary_total?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">{overview.secondary_pct}% share</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-red-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Higher Sec (11-12)</p>
                    <p className="text-2xl font-bold text-red-600">{overview.hs_total?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">{overview.hs_pct}% share</p>
                  </CardContent>
                </Card>
              </div>

              {/* Class-wise Gender Breakdown */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Class-wise Gender Distribution
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={classData} margin={{ left: 10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="class_name" tick={{ fontSize: 10 }} />
                        <YAxis />
                        <Tooltip content={<CustomTooltip />} />
                        <Legend />
                        <Bar dataKey="boys" name="Boys" fill="#3b82f6" stackId="a" />
                        <Bar dataKey="girls" name="Girls" fill="#ec4899" stackId="a" />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>

              {/* Class-wise Data Table */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Class-wise Detailed Breakdown
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow className="bg-slate-50">
                          <TableHead className="font-medium">Class</TableHead>
                          <TableHead className="font-medium text-right">Boys</TableHead>
                          <TableHead className="font-medium text-right">Girls</TableHead>
                          <TableHead className="font-medium text-right">Total</TableHead>
                          <TableHead className="font-medium text-right">Girls %</TableHead>
                          <TableHead className="font-medium text-right">GPI</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {classData.map((cls, idx) => (
                          <TableRow key={idx} className="hover:bg-slate-50">
                            <TableCell className="font-medium">{cls.class_name}</TableCell>
                            <TableCell className="text-right tabular-nums">{cls.boys?.toLocaleString()}</TableCell>
                            <TableCell className="text-right tabular-nums">{cls.girls?.toLocaleString()}</TableCell>
                            <TableCell className="text-right tabular-nums font-medium">{cls.total?.toLocaleString()}</TableCell>
                            <TableCell className="text-right tabular-nums">{cls.girls_pct}%</TableCell>
                            <TableCell className="text-right">
                              <Badge className={cls.gpi >= 0.95 ? "bg-emerald-100 text-emerald-700" : cls.gpi >= 0.85 ? "bg-amber-100 text-amber-700" : "bg-red-100 text-red-700"}>
                                {cls.gpi}
                              </Badge>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* DASHBOARD 3: GENDER & INCLUSION */}
            <TabsContent value="gender" className="space-y-6">
              {/* Gender KPIs */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <Card className="border-slate-200 border-l-4 border-l-blue-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Total Boys</p>
                    <p className="text-2xl font-bold text-blue-600">{overview.total_boys?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">{(100 - overview.girls_participation_pct).toFixed(1)}% of total</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-pink-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Total Girls</p>
                    <p className="text-2xl font-bold text-pink-600">{overview.total_girls?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">{overview.girls_participation_pct}% of total</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-emerald-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Gender Parity Index</p>
                    <p className="text-2xl font-bold text-emerald-600">{overview.gender_parity_index}</p>
                    <p className="text-sm text-slate-600">Target: 0.95-1.05</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-amber-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Gender Gap</p>
                    <p className="text-2xl font-bold text-amber-600">{overview.gender_gap?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">Boys - Girls</p>
                  </CardContent>
                </Card>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Gender Split Pie */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Overall Gender Distribution
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={[
                              { name: "Boys", value: overview.total_boys, color: "#3b82f6" },
                              { name: "Girls", value: overview.total_girls, color: "#ec4899" },
                              { name: "Trans", value: overview.total_trans || 0, color: "#8b5cf6" }
                            ].filter(d => d.value > 0)}
                            cx="50%"
                            cy="50%"
                            innerRadius={50}
                            outerRadius={80}
                            paddingAngle={2}
                            dataKey="value"
                            label={({ name, value }) => `${name}: ${value.toLocaleString()}`}
                          >
                            {[
                              { name: "Boys", value: overview.total_boys, color: "#3b82f6" },
                              { name: "Girls", value: overview.total_girls, color: "#ec4899" },
                              { name: "Trans", value: overview.total_trans || 0, color: "#8b5cf6" }
                            ].filter(d => d.value > 0).map((entry, index) => (
                              <Cell key={`cell-${index}`} fill={entry.color} />
                            ))}
                          </Pie>
                          <Tooltip />
                          <Legend />
                        </PieChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>

                {/* GPI by Class Line */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Gender Parity Index by Class
                    </CardTitle>
                    <CardDescription>Target range: 0.95-1.05</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={classData}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis dataKey="class_name" tick={{ fontSize: 10 }} />
                          <YAxis domain={[0.5, 1.5]} />
                          <Tooltip content={<CustomTooltip />} />
                          <Line type="monotone" dataKey="gpi" name="GPI" stroke="#10b981" strokeWidth={2} dot={{ r: 4 }} />
                          {/* Reference lines */}
                          <Line type="monotone" dataKey={() => 1} stroke="#94a3b8" strokeDasharray="5 5" dot={false} />
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Gender Gap by Class */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Gender Gap by Class (Boys - Girls)
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="h-72">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={classData.map(c => ({ ...c, gap: c.boys - c.girls }))}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="class_name" tick={{ fontSize: 10 }} />
                        <YAxis />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="gap" name="Gender Gap" radius={[4, 4, 0, 0]}>
                          {classData.map((entry, index) => (
                            <Cell 
                              key={`cell-${index}`} 
                              fill={entry.boys - entry.girls > 0 ? "#3b82f6" : "#ec4899"} 
                            />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* DASHBOARD 4: RETENTION & RISK */}
            <TabsContent value="retention" className="space-y-6">
              {/* Retention KPIs */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <Card className="border-slate-200">
                  <CardContent className="p-6">
                    <RetentionGauge 
                      value={overview.primary_upper_retention} 
                      label="Primary → Upper Primary"
                    />
                    <p className="text-xs text-slate-500 text-center mt-2">Class 5 → Class 6</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200">
                  <CardContent className="p-6">
                    <RetentionGauge 
                      value={overview.upper_secondary_retention} 
                      label="Upper Primary → Secondary"
                    />
                    <p className="text-xs text-slate-500 text-center mt-2">Class 8 → Class 9</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200">
                  <CardContent className="p-6">
                    <RetentionGauge 
                      value={overview.secondary_hs_retention} 
                      label="Secondary → Higher Sec"
                      target={80}
                    />
                    <p className="text-xs text-slate-500 text-center mt-2">Class 10 → Class 11 (Critical)</p>
                  </CardContent>
                </Card>
              </div>

              {/* Dropout Waterfall */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                    <AlertTriangle className="w-5 h-5 text-amber-500" />
                    Class-wise Dropout Analysis
                  </CardTitle>
                  <CardDescription>Red indicates &gt;15% drop (critical)</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <ComposedChart data={retentionData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="class_name" tick={{ fontSize: 10 }} />
                        <YAxis yAxisId="left" />
                        <YAxis yAxisId="right" orientation="right" domain={[0, 100]} />
                        <Tooltip content={<CustomTooltip />} />
                        <Legend />
                        <Bar yAxisId="left" dataKey="enrolment" name="Enrolment" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                        <Line yAxisId="right" type="monotone" dataKey="drop_pct" name="Drop %" stroke="#ef4444" strokeWidth={2} dot={{ r: 4 }} />
                      </ComposedChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Small Schools */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                      <AlertTriangle className="w-5 h-5 text-red-500" />
                      Small Schools (&lt;100 students)
                    </CardTitle>
                    <CardDescription>Viability risk - consider merge/upgrade</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-2 max-h-64 overflow-y-auto">
                      {smallSchools.slice(0, 10).map((school, idx) => (
                        <div key={idx} className="flex items-center justify-between p-2 bg-red-50 rounded-lg">
                          <div className="flex-1 min-w-0">
                            <p className="text-sm font-medium text-slate-900 truncate">
                              <SchoolLink udiseCode={school.udise_code}>{school.school_name}</SchoolLink>
                            </p>
                            <p className="text-xs text-slate-500">
                              <BlockLink blockCode={school.block_code}>{school.block_name}</BlockLink>
                            </p>
                          </div>
                          <Badge className="bg-red-100 text-red-700">{school.grand_total}</Badge>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>

                {/* Large Schools */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                      <School className="w-5 h-5 text-blue-500" />
                      Large Schools (&gt;1000 students)
                    </CardTitle>
                    <CardDescription>Infrastructure capacity check needed</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-2 max-h-64 overflow-y-auto">
                      {largeSchools.slice(0, 10).map((school, idx) => (
                        <div key={idx} className="flex items-center justify-between p-2 bg-blue-50 rounded-lg">
                          <div className="flex-1 min-w-0">
                            <p className="text-sm font-medium text-slate-900 truncate">
                              <SchoolLink udiseCode={school.udise_code}>{school.school_name}</SchoolLink>
                            </p>
                            <p className="text-xs text-slate-500">
                              <BlockLink blockCode={school.block_code}>{school.block_name}</BlockLink>
                            </p>
                          </div>
                          <Badge className="bg-blue-100 text-blue-700">{school.grand_total?.toLocaleString()}</Badge>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Block-wise Summary */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Block-wise Enrolment Summary
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow className="bg-slate-50">
                          <TableHead className="font-medium">Block</TableHead>
                          <TableHead className="font-medium text-right">Schools</TableHead>
                          <TableHead className="font-medium text-right">Enrolment</TableHead>
                          <TableHead className="font-medium text-right">GPI</TableHead>
                          <TableHead className="font-medium text-right">Avg Size</TableHead>
                          <TableHead className="font-medium text-right">Sec→HS Ret.</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {blockData.slice(0, 15).map((block, idx) => (
                          <TableRow key={idx} className="hover:bg-slate-50">
                            <TableCell className="font-medium">
                              <BlockLink blockCode={block.block_code}>{block.block_name}</BlockLink>
                            </TableCell>
                            <TableCell className="text-right tabular-nums">{block.total_schools}</TableCell>
                            <TableCell className="text-right tabular-nums">{block.grand_total?.toLocaleString()}</TableCell>
                            <TableCell className="text-right">
                              <Badge className={block.gpi >= 0.95 ? "bg-emerald-100 text-emerald-700" : "bg-amber-100 text-amber-700"}>
                                {block.gpi}
                              </Badge>
                            </TableCell>
                            <TableCell className="text-right tabular-nums">{block.avg_school_size}</TableCell>
                            <TableCell className="text-right">
                              <Badge className={block.secondary_retention >= 80 ? "bg-emerald-100 text-emerald-700" : "bg-red-100 text-red-700"}>
                                {block.secondary_retention}%
                              </Badge>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>
            <TabsContent value="insights" className="space-y-6">
              <AiInsightsTab
                title="Enrolment Analytics Insights"
                description="AI insights, root cause signals, recommendations, and priority actions for enrolment analytics."
                generate={buildInsights}
              />
            </TabsContent>
          </Tabs>
        </>
      )}
    </div>
  );
};

export default EnrolmentDashboard;
