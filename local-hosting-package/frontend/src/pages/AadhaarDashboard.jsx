import { useState, useEffect, useCallback } from "react";
import axios from "axios";
import { useScope } from "@/context/ScopeContext";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Input } from "@/components/ui/input";
import { Progress } from "@/components/ui/progress";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import { 
  ShieldCheck, 
  Users, 
  AlertTriangle, 
  CheckCircle2,
  XCircle,
  Clock,
  TrendingUp,
  TrendingDown,
  RefreshCw,
  Upload,
  School,
  Target,
  BarChart3,
  PieChart as PieChartIcon,
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
const KPICard = ({ label, value, suffix = "", icon: Icon, trend, trendValue, color = "blue", size = "default" }) => {
  const colors = {
    blue: "bg-blue-50 text-blue-600",
    green: "bg-emerald-50 text-emerald-600",
    red: "bg-red-50 text-red-600",
    amber: "bg-amber-50 text-amber-600",
    purple: "bg-purple-50 text-purple-600",
    slate: "bg-slate-50 text-slate-600",
  };

  return (
    <Card className="border-slate-200">
      <CardContent className={size === "large" ? "p-6" : "p-4"}>
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <p className="text-xs font-medium text-slate-500 uppercase tracking-wider">{label}</p>
            <div className="flex items-baseline gap-1 mt-1">
              <span className={`${size === "large" ? "text-3xl" : "text-2xl"} font-bold text-slate-900 tabular-nums`} style={{ fontFamily: 'Manrope' }}>
                {typeof value === 'number' ? value.toLocaleString('en-IN') : value}
              </span>
              {suffix && <span className="text-lg text-slate-500">{suffix}</span>}
            </div>
            {trend && trendValue && (
              <div className={`flex items-center gap-1 mt-1 text-sm ${trend === 'up' ? 'text-emerald-600' : trend === 'down' ? 'text-red-600' : 'text-slate-500'}`}>
                {trend === 'up' ? <TrendingUp className="w-3 h-3" /> : trend === 'down' ? <TrendingDown className="w-3 h-3" /> : null}
                <span>{trendValue}</span>
              </div>
            )}
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

// Gauge Component for Coverage %
const GaugeChart = ({ value, label, color = "#10b981" }) => {
  const angle = (value / 100) * 180;
  
  return (
    <div className="flex flex-col items-center">
      <div className="relative w-40 h-20 overflow-hidden">
        <div className="absolute w-40 h-40 rounded-full border-8 border-slate-200" style={{ clipPath: 'polygon(0 0, 100% 0, 100% 50%, 0 50%)' }} />
        <div 
          className="absolute w-40 h-40 rounded-full border-8 origin-center transition-transform duration-1000"
          style={{ 
            borderColor: color,
            clipPath: 'polygon(0 0, 100% 0, 100% 50%, 0 50%)',
            transform: `rotate(${angle - 180}deg)`,
            transformOrigin: 'center center'
          }} 
        />
        <div className="absolute inset-0 flex items-end justify-center pb-2">
          <span className="text-2xl font-bold text-slate-900" style={{ fontFamily: 'Manrope' }}>{value}%</span>
        </div>
      </div>
      <p className="text-sm text-slate-500 mt-2">{label}</p>
    </div>
  );
};

const AadhaarDashboard = () => {
  const { scope } = useScope();
  const [loading, setLoading] = useState(true);
  const [importing, setImporting] = useState(false);
  const [overview, setOverview] = useState(null);
  const [blockData, setBlockData] = useState([]);
  const [statusDistribution, setStatusDistribution] = useState(null);
  const [highRiskSchools, setHighRiskSchools] = useState([]);
  const [bottomBlocks, setBottomBlocks] = useState([]);
  const [paretoData, setParetoData] = useState([]);
  const [mbuData, setMbuData] = useState([]);
  const [activeTab, setActiveTab] = useState("executive");

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [
        overviewRes,
        blockRes,
        statusRes,
        highRiskRes,
        bottomRes,
        paretoRes,
        mbuRes
      ] = await Promise.all([
        axios.get(`${API}/aadhaar/overview`),
        axios.get(`${API}/aadhaar/block-wise`),
        axios.get(`${API}/aadhaar/status-distribution`),
        axios.get(`${API}/aadhaar/high-risk-schools`),
        axios.get(`${API}/aadhaar/bottom-blocks`),
        axios.get(`${API}/aadhaar/pareto-analysis`),
        axios.get(`${API}/aadhaar/mbu-analysis`)
      ]);
      
      setOverview(overviewRes.data);
      setBlockData(blockRes.data);
      setStatusDistribution(statusRes.data);
      setHighRiskSchools(highRiskRes.data);
      setBottomBlocks(bottomRes.data);
      setParetoData(paretoRes.data);
      setMbuData(mbuRes.data);
    } catch (error) {
      console.error("Error fetching Aadhaar data:", error);
    } finally {
      setLoading(false);
    }
  }, [scope.version]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handleImport = async () => {
    const url = "https://customer-assets.emergentagent.com/job_maharashtra-edu/artifacts/us8jnwni_1.%20Exceptional%20Data%20-%20AADHAAR%20Status%20-%20School%20Wise%20-%202025-26%20-%20%28State%29%20MAHARASHTRA.xlsx";
    
    setImporting(true);
    try {
      await axios.post(`${API}/aadhaar/import?url=${encodeURIComponent(url)}`);
      toast.success("Aadhaar data import started!");
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
              {entry.unit || ''}
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

  // Check if we have data
  const hasData = overview && overview.total_enrolment > 0;

  const buildInsights = () => {
    if (!overview) {
      return [
        "## Insights",
        "- No Aadhaar data available for the current filters.",
        "",
        "## Root Cause Signals",
        "- Missing or incomplete records in the selected scope.",
        "",
        "## Recommendations",
        "- Refresh data or broaden filters to include valid records.",
        "",
        "## Priority Action Items",
        "- Validate data import for Aadhaar analytics.",
      ].join("\n");
    }

    const fmt = (v) => (typeof v === "number" ? v.toLocaleString("en-IN") : v ?? 0);
    const pct = (v) => (typeof v === "number" ? `${v}%` : "0%");
    const blocks = Array.isArray(blockData) ? blockData : [];
    const byCoverageAsc = [...blocks]
      .filter((b) => b?.coverage_pct !== undefined)
      .sort((a, b) => (a.coverage_pct || 0) - (b.coverage_pct || 0));
    const byCoverageDesc = [...byCoverageAsc].reverse();
    const worst = byCoverageAsc.slice(0, 3);
    const best = byCoverageDesc.slice(0, 3);

    return [
      "## Insights",
      `- Aadhaar coverage is **${pct(overview.aadhaar_coverage_pct)}** across **${fmt(overview.total_enrolment)}** students.`,
      `- Exception rate is **${pct(overview.aadhaar_exception_pct)}** and name mismatch is **${pct(overview.name_mismatch_pct)}**.`,
      worst.length ? `- Lowest coverage blocks: **${worst.map((b) => b.block_name).join(", ")}**.` : "- Lowest coverage blocks: unavailable.",
      best.length ? `- Top performing blocks: **${best.map((b) => b.block_name).join(", ")}**.` : "- Top performing blocks: unavailable.",
      "",
      "## Root Cause Signals",
      `- Pending Aadhaar accounts: **${fmt(overview.aadhaar_pending)}**; failed: **${fmt(overview.aadhaar_failed)}**.`,
      `- Not provided cases: **${fmt(overview.aadhaar_not_provided)}** indicate documentation gaps.`,
      "",
      "## Recommendations",
      "- Prioritize exception resolution and name mismatch corrections in low-coverage blocks.",
      "- Run weekly school-level verification drives to reduce pending Aadhaar cases.",
      "",
      "## Priority Action Items",
      worst.length ? `- Week 1: fix Aadhaar coverage in **${worst.map((b) => b.block_name).join(", ")}**; target **+5pp** by clearing pending lists.` : "- Week 1: fix Aadhaar coverage in lowest blocks; target +5pp by clearing pending lists.",
      `- Week 2: resolve **${fmt(overview.aadhaar_failed)}** failed cases with document re-verification.`,
      `- Week 3–4: reduce **${fmt(overview.aadhaar_pending)}** pending + **${fmt(overview.aadhaar_not_provided)}** not provided cases to zero.`,
    ].join("\n");
  };

  return (
    <div className="space-y-6 animate-fade-in" data-testid="aadhaar-dashboard">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight" style={{ fontFamily: 'Manrope' }}>
            Aadhaar Analytics Dashboard
          </h1>
          <p className="text-slate-500 mt-1">School-wise Aadhaar Status • AY 2025-26 • Maharashtra</p>
        </div>
        <div className="flex items-center gap-3">
          <Button 
            variant="outline" 
            size="sm" 
            onClick={handleImport}
            disabled={importing}
            data-testid="import-aadhaar-btn"
          >
            {importing ? <RefreshCw className="w-4 h-4 mr-2 animate-spin" /> : <Upload className="w-4 h-4 mr-2" />}
            {importing ? "Importing..." : "Import Data"}
          </Button>
          <ExportPanel dashboardName="aadhaar" dashboardTitle="Aadhaar Analytics" />
          <Button variant="outline" size="sm" onClick={fetchData}>
            <RefreshCw className="w-4 h-4 mr-2" />
            Refresh
          </Button>
        </div>
      </div>

      {!hasData ? (
        <Card className="border-slate-200">
          <CardContent className="py-12 text-center">
            <ShieldCheck className="w-16 h-16 mx-auto text-slate-300 mb-4" />
            <h3 className="text-xl font-semibold text-slate-700 mb-2">No Aadhaar Data Available</h3>
            <p className="text-slate-500 mb-4">Click "Import Data" to load the Aadhaar status Excel file</p>
            <Button onClick={handleImport} disabled={importing}>
              {importing ? "Importing..." : "Import Aadhaar Data"}
            </Button>
          </CardContent>
        </Card>
      ) : (
        <>
          {/* Dashboard Tabs */}
          <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
            <TabsList className="bg-slate-100">
              <TabsTrigger value="executive" className="flex items-center gap-2">
                <BarChart3 className="w-4 h-4" />
                Executive Overview
              </TabsTrigger>
              <TabsTrigger value="operational" className="flex items-center gap-2">
                <Activity className="w-4 h-4" />
                Operational Control
              </TabsTrigger>
              <TabsTrigger value="action" className="flex items-center gap-2">
                <Target className="w-4 h-4" />
                Action & Accountability
              </TabsTrigger>
              <TabsTrigger value="insights" className="flex items-center gap-2">
                <Brain className="w-4 h-4" />
                Insights
              </TabsTrigger>
            </TabsList>

            {/* DASHBOARD 1: EXECUTIVE OVERVIEW */}
            <TabsContent value="executive" className="space-y-6">
              {/* Top KPI Strip */}
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
                <KPICard
                  label="Total Students"
                  value={overview.total_enrolment}
                  icon={Users}
                  color="blue"
                  size="large"
                />
                <KPICard
                  label="Aadhaar Coverage"
                  value={overview.aadhaar_coverage_pct}
                  suffix="%"
                  icon={ShieldCheck}
                  color="green"
                  size="large"
                />
                <KPICard
                  label="Exception Rate"
                  value={overview.aadhaar_exception_pct}
                  suffix="%"
                  icon={AlertTriangle}
                  color="red"
                  size="large"
                />
                <KPICard
                  label="Name Mismatch"
                  value={overview.name_mismatch_pct}
                  suffix="%"
                  icon={XCircle}
                  color="amber"
                  size="large"
                />
                <KPICard
                  label="MBU Pending"
                  value={overview.mbu_pending_total_pct}
                  suffix="%"
                  icon={Clock}
                  color="purple"
                  size="large"
                />
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* Aadhaar Status Donut */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Aadhaar Status Distribution
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-72">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={statusDistribution?.distribution || []}
                            cx="50%"
                            cy="50%"
                            innerRadius={60}
                            outerRadius={90}
                            paddingAngle={2}
                            dataKey="value"
                            label={({ name, percentage }) => `${percentage}%`}
                          >
                            {(statusDistribution?.distribution || []).map((entry, index) => (
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

                {/* Block-wise Coverage Bar */}
                <Card className="border-slate-200 lg:col-span-2">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Block-wise Aadhaar Coverage %
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-72">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={blockData.slice(0, 15)} layout="vertical" margin={{ left: 80 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis type="number" domain={[0, 100]} />
                          <YAxis dataKey="block_name" type="category" width={70} tick={{ fontSize: 11 }} />
                          <Tooltip content={<CustomTooltip />} />
                          <Bar dataKey="aadhaar_coverage_pct" name="Coverage %" radius={[0, 4, 4, 0]}>
                            {blockData.slice(0, 15).map((entry, index) => (
                              <Cell 
                                key={`cell-${index}`} 
                                fill={entry.aadhaar_coverage_pct >= 90 ? "#10b981" : entry.aadhaar_coverage_pct >= 80 ? "#f59e0b" : "#ef4444"} 
                              />
                            ))}
                          </Bar>
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Pareto Chart */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Exception Contribution by Block (Pareto Analysis)
                  </CardTitle>
                  <CardDescription>20% of blocks often contribute ~80% of exceptions</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <ComposedChart data={paretoData.slice(0, 20)} margin={{ left: 20, right: 20 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="block_name" tick={{ fontSize: 10 }} angle={-45} textAnchor="end" height={80} />
                        <YAxis yAxisId="left" orientation="left" label={{ value: 'Exceptions', angle: -90, position: 'insideLeft' }} />
                        <YAxis yAxisId="right" orientation="right" domain={[0, 100]} label={{ value: 'Cumulative %', angle: 90, position: 'insideRight' }} />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar yAxisId="left" dataKey="exceptions" name="Exceptions" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                        <Line yAxisId="right" type="monotone" dataKey="cumulative_pct" name="Cumulative %" stroke="#ef4444" strokeWidth={2} dot={{ r: 3 }} />
                      </ComposedChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* DASHBOARD 2: OPERATIONAL CONTROL */}
            <TabsContent value="operational" className="space-y-6">
              {/* Stacked Bar: Aadhaar Issues by Block */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Aadhaar Issues by Block (Stacked)
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="h-96">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={blockData.slice(0, 15)} layout="vertical" margin={{ left: 80 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis type="number" />
                        <YAxis dataKey="block_name" type="category" width={70} tick={{ fontSize: 11 }} />
                        <Tooltip content={<CustomTooltip />} />
                        <Legend />
                        <Bar dataKey="aadhaar_failed" name="Failed" stackId="a" fill="#ef4444" />
                        <Bar dataKey="aadhaar_pending" name="Pending" stackId="a" fill="#f59e0b" />
                        <Bar dataKey="aadhaar_not_provided" name="Not Provided" stackId="a" fill="#6b7280" />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Name Mismatch Heatmap */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                      <AlertTriangle className="w-5 h-5 text-red-500" />
                      Name Mismatch Risk by Block
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-2">
                      {blockData.slice(0, 10).map((block, idx) => (
                        <div key={idx} className="flex items-center gap-3">
                          <span className="w-24 text-sm text-slate-600 truncate">{block.block_name}</span>
                          <div className="flex-1 h-6 bg-slate-100 rounded-full overflow-hidden">
                            <div 
                              className={`h-full transition-all ${
                                block.name_mismatch_pct > 10 ? 'bg-red-500' : 
                                block.name_mismatch_pct > 5 ? 'bg-amber-500' : 'bg-emerald-500'
                              }`}
                              style={{ width: `${Math.min(block.name_mismatch_pct, 100)}%` }}
                            />
                          </div>
                          <span className="w-16 text-sm font-medium text-right">{block.name_mismatch_pct}%</span>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>

                {/* MBU Pending by Age */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      MBU Pending (Age-wise) by Block
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-72">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={mbuData.slice(0, 10)} layout="vertical" margin={{ left: 80 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis type="number" />
                          <YAxis dataKey="block_name" type="category" width={70} tick={{ fontSize: 11 }} />
                          <Tooltip content={<CustomTooltip />} />
                          <Legend />
                          <Bar dataKey="mbu_5_15" name="Age 5-15" stackId="a" fill="#8b5cf6" />
                          <Bar dataKey="mbu_15_plus" name="Age 15+" stackId="a" fill="#06b6d4" />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* High Risk Schools Table */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                    <AlertTriangle className="w-5 h-5 text-red-500" />
                    Top 20 High-Risk Schools (by Exception Rate)
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow className="bg-slate-50">
                          <TableHead className="font-medium">Rank</TableHead>
                          <TableHead className="font-medium">School Name</TableHead>
                          <TableHead className="font-medium">Block</TableHead>
                          <TableHead className="font-medium text-right">Enrolment</TableHead>
                          <TableHead className="font-medium text-right">Exception Rate</TableHead>
                          <TableHead className="font-medium text-right">Coverage %</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {highRiskSchools.map((school, idx) => (
                          <TableRow key={idx} className="hover:bg-slate-50">
                            <TableCell className="font-medium">
                              <Badge variant="destructive" className="w-6 h-6 rounded-full p-0 flex items-center justify-center">
                                {idx + 1}
                              </Badge>
                            </TableCell>
                            <TableCell className="max-w-xs truncate">
                              <SchoolLink udiseCode={school.udise_code}>{school.school_name}</SchoolLink>
                            </TableCell>
                            <TableCell>
                              <BlockLink blockCode={school.block_code}>{school.block_name}</BlockLink>
                            </TableCell>
                            <TableCell className="text-right tabular-nums">{school.total_enrolment?.toLocaleString()}</TableCell>
                            <TableCell className="text-right">
                              <Badge className={`${school.exception_rate > 20 ? 'bg-red-100 text-red-700' : 'bg-amber-100 text-amber-700'}`}>
                                {school.exception_rate}%
                              </Badge>
                            </TableCell>
                            <TableCell className="text-right tabular-nums">{school.aadhaar_coverage_pct}%</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* DASHBOARD 3: ACTION & ACCOUNTABILITY */}
            <TabsContent value="action" className="space-y-6">
              {/* KPI Summary Cards - At Top */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <Card className="border-slate-200 border-l-4 border-l-emerald-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Primary KPI</p>
                    <p className="text-xl font-bold text-emerald-600">{overview.aadhaar_coverage_pct}%</p>
                    <p className="text-sm text-slate-600">Aadhaar Coverage</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-red-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Highest Risk</p>
                    <p className="text-xl font-bold text-red-600">{overview.name_mismatch_pct}%</p>
                    <p className="text-sm text-slate-600">Name Mismatch</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-purple-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Operational Load</p>
                    <p className="text-xl font-bold text-purple-600">{overview.mbu_pending_total_pct}%</p>
                    <p className="text-sm text-slate-600">MBU Pending</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-amber-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Schools at Risk</p>
                    <p className="text-xl font-bold text-amber-600">{overview.high_risk_schools}</p>
                    <p className="text-sm text-slate-600">Exception &gt;10%</p>
                  </CardContent>
                </Card>
              </div>

              {/* Performance Index Leaderboard */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Block Performance Index (Leaderboard)
                  </CardTitle>
                  <CardDescription>
                    Score = 0.5×Coverage + 0.3×NameMatch + 0.2×(100-MBU%)
                  </CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-96">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart 
                        data={[...blockData].sort((a, b) => b.performance_index - a.performance_index).slice(0, 15)} 
                        layout="vertical" 
                        margin={{ left: 80 }}
                      >
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis type="number" domain={[0, 100]} />
                        <YAxis dataKey="block_name" type="category" width={70} tick={{ fontSize: 11 }} />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="performance_index" name="Performance Index" radius={[0, 4, 4, 0]}>
                          {[...blockData].sort((a, b) => b.performance_index - a.performance_index).slice(0, 15).map((entry, index) => (
                            <Cell 
                              key={`cell-${index}`} 
                              fill={index < 5 ? "#10b981" : index < 10 ? "#3b82f6" : "#f59e0b"} 
                            />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Bottom 10 Blocks */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                      <TrendingDown className="w-5 h-5 text-red-500" />
                      Bottom 10 Blocks (Lowest Coverage)
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-3">
                      {bottomBlocks.map((block, idx) => (
                        <div key={idx} className="flex items-center justify-between p-3 bg-red-50 rounded-lg">
                          <div className="flex items-center gap-3">
                            <span className="w-6 h-6 flex items-center justify-center bg-red-100 text-red-700 text-xs font-bold rounded-full">
                              {idx + 1}
                            </span>
                            <div>
                              <p className="font-medium text-slate-900">{block.block_name}</p>
                              <p className="text-xs text-slate-500">{block.total_schools} schools • {block.total_enrolment?.toLocaleString()} students</p>
                            </div>
                          </div>
                          <Badge className="bg-red-100 text-red-700">{block.coverage_pct}%</Badge>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>

                {/* Compliance Gap */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Compliance Gap Analysis
                    </CardTitle>
                    <CardDescription>Students needed to reach target coverage</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-6">
                      <div className="text-center p-6 bg-slate-50 rounded-lg">
                        <p className="text-sm text-slate-500 mb-2">Gap to 100% Coverage</p>
                        <p className="text-4xl font-bold text-slate-900" style={{ fontFamily: 'Manrope' }}>
                          {(overview.total_enrolment - overview.aadhaar_passed).toLocaleString()}
                        </p>
                        <p className="text-sm text-slate-500 mt-2">students remaining</p>
                      </div>
                      
                      <div className="space-y-3">
                        <div className="flex items-center justify-between">
                          <span className="text-sm text-slate-600">Current Coverage</span>
                          <span className="font-bold text-emerald-600">{overview.aadhaar_coverage_pct}%</span>
                        </div>
                        <Progress value={overview.aadhaar_coverage_pct} className="h-3" />
                        <div className="flex items-center justify-between text-xs text-slate-500">
                          <span>{overview.aadhaar_passed?.toLocaleString()} covered</span>
                          <span>Target: 100%</span>
                        </div>
                      </div>

                      <div className="grid grid-cols-3 gap-4 pt-4 border-t">
                        <div className="text-center">
                          <p className="text-xl font-bold text-red-600">{overview.aadhaar_failed?.toLocaleString()}</p>
                          <p className="text-xs text-slate-500">Failed</p>
                        </div>
                        <div className="text-center">
                          <p className="text-xl font-bold text-amber-600">{overview.aadhaar_pending?.toLocaleString()}</p>
                          <p className="text-xs text-slate-500">Pending</p>
                        </div>
                        <div className="text-center">
                          <p className="text-xl font-bold text-slate-600">{overview.aadhaar_not_provided?.toLocaleString()}</p>
                          <p className="text-xs text-slate-500">Not Provided</p>
                        </div>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              </div>
            </TabsContent>

            <TabsContent value="insights" className="space-y-6">
              <AiInsightsTab
                title="Aadhaar Analytics Insights"
                description="AI insights, root cause signals, recommendations, and priority actions for Aadhaar coverage."
                generate={buildInsights}
              />
            </TabsContent>
          </Tabs>
        </>
      )}
    </div>
  );
};

export default AadhaarDashboard;
