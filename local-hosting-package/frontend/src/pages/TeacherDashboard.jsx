import { useState, useEffect, useCallback } from "react";
import axios from "axios";
import { useScope } from "@/context/ScopeContext";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
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
  Users, 
  GraduationCap,
  AlertTriangle, 
  TrendingUp,
  TrendingDown,
  RefreshCw,
  Upload,
  School,
  Target,
  BarChart3,
  Activity,
  Award,
  Monitor,
  UserCheck,
  ArrowUpRight,
  ArrowDownRight,
  Minus,
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
  ComposedChart,
  Line,
  RadarChart,
  Radar,
  PolarGrid,
  PolarAngleAxis,
  PolarRadiusAxis
} from "recharts";

import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;

// KPI Card Component
const KPICard = ({ label, value, suffix = "", icon: Icon, trend, trendValue, color = "blue", size = "default", description }) => {
  const colors = {
    blue: "bg-blue-50 text-blue-600",
    green: "bg-emerald-50 text-emerald-600",
    red: "bg-red-50 text-red-600",
    amber: "bg-amber-50 text-amber-600",
    purple: "bg-purple-50 text-purple-600",
    slate: "bg-slate-50 text-slate-600",
    cyan: "bg-cyan-50 text-cyan-600",
  };

  return (
    <Card className="border-slate-200" data-testid={`kpi-${label.toLowerCase().replace(/\s+/g, '-')}`}>
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
            {trend !== undefined && trendValue !== undefined && (
              <div className={`flex items-center gap-1 mt-1 text-sm ${trend === 'up' ? 'text-emerald-600' : trend === 'down' ? 'text-red-600' : 'text-slate-500'}`}>
                {trend === 'up' ? <TrendingUp className="w-3 h-3" /> : trend === 'down' ? <TrendingDown className="w-3 h-3" /> : <Minus className="w-3 h-3" />}
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

// Gauge Component
const GaugeBar = ({ value, label, color = "#10b981", maxValue = 100 }) => {
  const percentage = Math.min((value / maxValue) * 100, 100);
  
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <span className="text-sm text-slate-600">{label}</span>
        <span className="text-sm font-bold" style={{ color }}>{value}%</span>
      </div>
      <div className="h-2 bg-slate-100 rounded-full overflow-hidden">
        <div 
          className="h-full rounded-full transition-all duration-500"
          style={{ width: `${percentage}%`, backgroundColor: color }}
        />
      </div>
    </div>
  );
};

const TeacherDashboard = () => {
  const { scope } = useScope();
  const [loading, setLoading] = useState(true);
  const [importing, setImporting] = useState(false);
  const [overview, setOverview] = useState(null);
  const [blockData, setBlockData] = useState([]);
  const [schoolDistribution, setSchoolDistribution] = useState([]);
  const [topGains, setTopGains] = useState([]);
  const [topLosses, setTopLosses] = useState([]);
  const [trainingCoverage, setTrainingCoverage] = useState([]);
  const [qualificationRisk, setQualificationRisk] = useState([]);
  const [deploymentRisk, setDeploymentRisk] = useState([]);
  const [blockComparison, setBlockComparison] = useState([]);
  const [activeTab, setActiveTab] = useState("executive");

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [
        overviewRes,
        blockRes,
        distributionRes,
        gainsRes,
        lossesRes,
        trainingRes,
        qualRes,
        deployRes,
        comparisonRes
      ] = await Promise.all([
        axios.get(`${API}/teacher/overview`),
        axios.get(`${API}/teacher/block-wise`),
        axios.get(`${API}/teacher/school-distribution`),
        axios.get(`${API}/teacher/top-changes?change_type=gain`),
        axios.get(`${API}/teacher/top-changes?change_type=loss`),
        axios.get(`${API}/teacher/training-coverage`),
        axios.get(`${API}/teacher/qualification-risk`),
        axios.get(`${API}/teacher/deployment-risk`),
        axios.get(`${API}/teacher/block-comparison`)
      ]);
      
      setOverview(overviewRes.data);
      setBlockData(blockRes.data);
      setSchoolDistribution(distributionRes.data);
      setTopGains(gainsRes.data);
      setTopLosses(lossesRes.data);
      setTrainingCoverage(trainingRes.data);
      setQualificationRisk(qualRes.data);
      setDeploymentRisk(deployRes.data);
      setBlockComparison(comparisonRes.data);
    } catch (error) {
      console.error("Error fetching Teacher data:", error);
      toast.error("Failed to load teacher data");
    } finally {
      setLoading(false);
    }
  }, [scope.version]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handleImport = async () => {
    const url = "https://customer-assets.emergentagent.com/job_7d6cbc1e-b567-4fbc-af84-06fa5107bbd4/artifacts/85yv9qfh_2.%20MAHARASHTRA_School_Wise_Comparison_AY_2025-26.xlsx";
    
    setImporting(true);
    try {
      await axios.post(`${API}/teacher/import?url=${encodeURIComponent(url)}`);
      toast.success("Teacher data import started!");
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

  const hasData = overview && overview.teachers_cy > 0;

  const buildInsights = () => {
    if (!overview) {
      return [
        "## Insights",
        "- No teacher data available for the current filters.",
        "",
        "## Root Cause Signals",
        "- Missing or incomplete records in the selected scope.",
        "",
        "## Recommendations",
        "- Refresh data or broaden filters to include valid records.",
        "",
        "## Priority Action Items",
        "- Validate data import for teacher analytics.",
      ].join("\n");
    }

    const fmt = (v) => (typeof v === "number" ? v.toLocaleString("en-IN") : v ?? 0);
    const pct = (v) => (typeof v === "number" ? `${v}%` : "0%");
    const blocks = Array.isArray(blockData) ? blockData : [];
    const riskTop = [...blocks].sort((a, b) => (b.risk_index || 0) - (a.risk_index || 0)).slice(0, 3);
    const qualityTop = [...blocks].sort((a, b) => (b.quality_index || 0) - (a.quality_index || 0)).slice(0, 3);

    return [
      "## Insights",
      `- Total teachers: **${fmt(overview.teachers_cy)}**, growth: **${pct(overview.teacher_growth_pct)}**.`,
      `- Avg teachers per school: **${fmt(overview.avg_teachers_per_school)}**.`,
      `- CTET coverage: **${pct(overview.ctet_coverage_pct)}**, quality index: **${fmt(overview.teacher_quality_index)}**.`,
      riskTop.length ? `- Highest risk blocks: **${riskTop.map((b) => b.block_name).join(", ")}**.` : "- Highest risk blocks: unavailable.",
      "",
      "## Root Cause Signals",
      `- Teacher risk index at **${fmt(overview.teacher_risk_index)}** suggests shortages/retirements in select blocks.`,
      `- Below graduation share: **${pct(overview.below_grad_pct)}** indicates qualification gaps.`,
      "",
      "## Recommendations",
      "- Prioritize recruitment or redeployment for high-risk blocks.",
      "- Increase CTET and subject training coverage in low-quality blocks.",
      "",
      "## Priority Action Items",
      riskTop.length ? `- Week 1: staff the highest-risk blocks **${riskTop.map((b) => b.block_name).join(", ")}** to cut risk index.` : "- Week 1: staff the highest-risk blocks to cut risk index.",
      `- Week 2: raise CTET coverage from **${pct(overview.ctet_coverage_pct)}** by running training + exam prep.`,
      `- Week 3–4: close vacancies to improve avg teachers/school (**${fmt(overview.avg_teachers_per_school)}**).`,
    ].join("\n");
  };

  // Prepare radar data for block readiness
  const radarData = blockData.slice(0, 5).map(block => ({
    block: block.block_name?.substring(0, 10) || '',
    "CTET %": block.ctet_pct || 0,
    "CWSN %": block.cwsn_pct || 0,
    "Digital %": block.computer_pct || 0,
    "Quality Index": block.quality_index || 0
  }));

  return (
    <div className="space-y-6 animate-fade-in" data-testid="teacher-dashboard">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight" style={{ fontFamily: 'Manrope' }}>
            Teacher Analytics Dashboard
          </h1>
          <p className="text-slate-500 mt-1">School-wise Teacher Comparison • AY 2025-26 • Maharashtra</p>
        </div>
        <div className="flex items-center gap-3">
          <Button 
            variant="outline" 
            size="sm" 
            onClick={handleImport}
            disabled={importing}
            data-testid="import-teacher-btn"
          >
            {importing ? <RefreshCw className="w-4 h-4 mr-2 animate-spin" /> : <Upload className="w-4 h-4 mr-2" />}
            {importing ? "Importing..." : "Import Data"}
          </Button>
          <ExportPanel dashboardName="teacher" dashboardTitle="Teacher Analytics" />
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
            <h3 className="text-xl font-semibold text-slate-700 mb-2">No Teacher Data Available</h3>
            <p className="text-slate-500 mb-4">Click "Import Data" to load the Teacher Comparison Excel file</p>
            <Button onClick={handleImport} disabled={importing} data-testid="import-teacher-empty-btn">
              {importing ? "Importing..." : "Import Teacher Data"}
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
              <TabsTrigger value="quality" className="flex items-center gap-2" data-testid="tab-quality">
                <Award className="w-4 h-4" />
                Quality & Training
              </TabsTrigger>
              <TabsTrigger value="deployment" className="flex items-center gap-2" data-testid="tab-deployment">
                <Activity className="w-4 h-4" />
                Deployment & Risk
              </TabsTrigger>
              <TabsTrigger value="drilldown" className="flex items-center gap-2" data-testid="tab-drilldown">
                <Target className="w-4 h-4" />
                School Drilldown
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
                  label="Total Teachers (CY)"
                  value={overview.teachers_cy}
                  icon={Users}
                  color="blue"
                  trend={overview.teacher_growth >= 0 ? "up" : "down"}
                  trendValue={`${overview.teacher_growth >= 0 ? '+' : ''}${overview.teacher_growth}`}
                />
                <KPICard
                  label="Growth Rate"
                  value={overview.teacher_growth_pct}
                  suffix="%"
                  icon={TrendingUp}
                  color={overview.teacher_growth_pct >= 0 ? "green" : "red"}
                />
                <KPICard
                  label="Avg/School"
                  value={overview.avg_teachers_per_school}
                  icon={School}
                  color="purple"
                />
                <KPICard
                  label="Quality Index"
                  value={overview.teacher_quality_index}
                  icon={Award}
                  color="green"
                  description="TQI Score"
                />
                <KPICard
                  label="Risk Index"
                  value={overview.teacher_risk_index}
                  icon={AlertTriangle}
                  color={overview.teacher_risk_index > 10 ? "red" : "amber"}
                  description="TRI Score"
                />
                <KPICard
                  label="CTET Coverage"
                  value={overview.ctet_coverage_pct}
                  suffix="%"
                  icon={UserCheck}
                  color="cyan"
                />
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                {/* School Distribution Pie */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Staffing Change Distribution
                    </CardTitle>
                    <CardDescription>Schools by YoY staffing change</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-72">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={schoolDistribution}
                            cx="50%"
                            cy="50%"
                            innerRadius={50}
                            outerRadius={80}
                            paddingAngle={2}
                            dataKey="value"
                            label={({ name, value }) => `${value}`}
                          >
                            {schoolDistribution.map((entry, index) => (
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

                {/* Block-wise Teacher Count */}
                <Card className="border-slate-200 lg:col-span-2">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Block-wise Teacher Strength (PY vs CY)
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-72">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={blockComparison.slice(0, 12)} margin={{ left: 10 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis dataKey="block_name" tick={{ fontSize: 10 }} angle={-45} textAnchor="end" height={60} />
                          <YAxis />
                          <Tooltip content={<CustomTooltip />} />
                          <Legend />
                          <Bar dataKey="teachers_py" name="Previous Year" fill="#94a3b8" radius={[4, 4, 0, 0]} />
                          <Bar dataKey="teachers_cy" name="Current Year" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Block Growth Chart */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Block-wise Teacher Growth Rate (%)
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={blockData.slice(0, 15)} layout="vertical" margin={{ left: 80 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis type="number" />
                        <YAxis dataKey="block_name" type="category" width={70} tick={{ fontSize: 11 }} />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="teacher_growth_pct" name="Growth %" radius={[0, 4, 4, 0]}>
                          {blockData.slice(0, 15).map((entry, index) => (
                            <Cell 
                              key={`cell-${index}`} 
                              fill={entry.teacher_growth_pct >= 0 ? "#10b981" : "#ef4444"} 
                            />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* DASHBOARD 2: QUALITY & TRAINING */}
            <TabsContent value="quality" className="space-y-6">
              {/* Training KPIs */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <Card className="border-slate-200 border-l-4 border-l-blue-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">CTET Qualified</p>
                    <p className="text-2xl font-bold text-blue-600">{overview.ctet_cy?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">{overview.ctet_coverage_pct}% coverage</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-purple-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">CWSN Trained</p>
                    <p className="text-2xl font-bold text-purple-600">{overview.cwsn_trained_cy?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">{overview.cwsn_coverage_pct}% coverage</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-cyan-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Computer Trained</p>
                    <p className="text-2xl font-bold text-cyan-600">{overview.computer_trained_cy?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">{overview.digital_readiness_pct}% coverage</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-red-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Below Graduation</p>
                    <p className="text-2xl font-bold text-red-600">{overview.below_grad_cy?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">{overview.below_grad_pct}% of teachers</p>
                  </CardContent>
                </Card>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Training Coverage Gauges */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Training Coverage Overview
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-6">
                    {trainingCoverage.map((item, idx) => (
                      <GaugeBar 
                        key={idx} 
                        value={item.percentage} 
                        label={`${item.name} (${item.trained?.toLocaleString()} teachers)`} 
                        color={item.color} 
                      />
                    ))}
                    <div className="pt-4 border-t">
                      <GaugeBar 
                        value={overview.teacher_quality_index} 
                        label="Teacher Quality Index (TQI)" 
                        color="#10b981" 
                      />
                    </div>
                  </CardContent>
                </Card>

                {/* Below Graduation Risk by Block */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                      <AlertTriangle className="w-5 h-5 text-red-500" />
                      Qualification Risk by Block
                    </CardTitle>
                    <CardDescription>Teachers below graduation level</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-72">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={qualificationRisk.slice(0, 10)} layout="vertical" margin={{ left: 80 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis type="number" />
                          <YAxis dataKey="block_name" type="category" width={70} tick={{ fontSize: 11 }} />
                          <Tooltip content={<CustomTooltip />} />
                          <Bar dataKey="below_grad_cy" name="Below Grad" fill="#ef4444" radius={[0, 4, 4, 0]} />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Block-wise Quality Index */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Block-wise Teacher Quality Index (Leaderboard)
                  </CardTitle>
                  <CardDescription>TQI = (CTET% × 0.4) + (CWSN% × 0.3) + (Computer% × 0.3)</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart 
                        data={[...blockData].sort((a, b) => b.quality_index - a.quality_index).slice(0, 15)} 
                        layout="vertical" 
                        margin={{ left: 80 }}
                      >
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis type="number" domain={[0, 100]} />
                        <YAxis dataKey="block_name" type="category" width={70} tick={{ fontSize: 11 }} />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="quality_index" name="Quality Index" radius={[0, 4, 4, 0]}>
                          {[...blockData].sort((a, b) => b.quality_index - a.quality_index).slice(0, 15).map((entry, index) => (
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
            </TabsContent>

            {/* DASHBOARD 3: DEPLOYMENT & RISK */}
            <TabsContent value="deployment" className="space-y-6">
              {/* Risk KPIs at Top */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <Card className="border-slate-200 border-l-4 border-l-amber-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">On Deputation</p>
                    <p className="text-2xl font-bold text-amber-600">{overview.deputation_cy?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">{overview.deputation_ratio}% of teachers</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-purple-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Cross-School Teaching</p>
                    <p className="text-2xl font-bold text-purple-600">{overview.other_school_cy?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">{overview.cross_school_ratio}% of teachers</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-red-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Teacher Risk Index</p>
                    <p className="text-2xl font-bold text-red-600">{overview.teacher_risk_index}</p>
                    <p className="text-sm text-slate-600">TRI Score</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-slate-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Total Schools</p>
                    <p className="text-2xl font-bold text-slate-600">{overview.total_schools?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">in dataset</p>
                  </CardContent>
                </Card>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Deployment Risk by Block */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Deputation by Block
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-72">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={deploymentRisk.slice(0, 10)} layout="vertical" margin={{ left: 80 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis type="number" />
                          <YAxis dataKey="block_name" type="category" width={70} tick={{ fontSize: 11 }} />
                          <Tooltip content={<CustomTooltip />} />
                          <Legend />
                          <Bar dataKey="deputation_cy" name="On Deputation" fill="#f59e0b" />
                          <Bar dataKey="other_school_cy" name="Other Schools" fill="#8b5cf6" />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>

                {/* Risk Index Heatmap */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                      <AlertTriangle className="w-5 h-5 text-red-500" />
                      Block Risk Index
                    </CardTitle>
                    <CardDescription>TRI = (BelowGrad% × 0.4) + (Deputation% × 0.3) + (CrossSchool% × 0.3)</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="space-y-2">
                      {[...blockData].sort((a, b) => b.risk_index - a.risk_index).slice(0, 10).map((block, idx) => (
                        <div key={idx} className="flex items-center gap-3">
                          <span className="w-24 text-sm text-slate-600 truncate">{block.block_name}</span>
                          <div className="flex-1 h-6 bg-slate-100 rounded-full overflow-hidden">
                            <div 
                              className={`h-full transition-all ${
                                block.risk_index > 15 ? 'bg-red-500' : 
                                block.risk_index > 8 ? 'bg-amber-500' : 'bg-emerald-500'
                              }`}
                              style={{ width: `${Math.min(block.risk_index * 2, 100)}%` }}
                            />
                          </div>
                          <span className="w-16 text-sm font-medium text-right">{block.risk_index}</span>
                        </div>
                      ))}
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Staffing Variance Chart */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Block Staffing Analysis (Teachers per School)
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <ComposedChart data={blockData.slice(0, 15)} margin={{ left: 10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="block_name" tick={{ fontSize: 10 }} angle={-45} textAnchor="end" height={80} />
                        <YAxis yAxisId="left" orientation="left" />
                        <YAxis yAxisId="right" orientation="right" />
                        <Tooltip content={<CustomTooltip />} />
                        <Legend />
                        <Bar yAxisId="left" dataKey="teachers_cy" name="Total Teachers" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                        <Line yAxisId="right" type="monotone" dataKey="avg_teachers_per_school" name="Avg/School" stroke="#ef4444" strokeWidth={2} dot={{ r: 4 }} />
                      </ComposedChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* DASHBOARD 4: SCHOOL DRILLDOWN */}
            <TabsContent value="drilldown" className="space-y-6">
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Top Gains */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                      <ArrowUpRight className="w-5 h-5 text-emerald-500" />
                      Top 10 Schools - Highest Staffing Gain
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="overflow-x-auto">
                      <Table>
                        <TableHeader>
                          <TableRow className="bg-slate-50">
                            <TableHead className="font-medium">School</TableHead>
                            <TableHead className="font-medium">Block</TableHead>
                            <TableHead className="font-medium text-right">PY</TableHead>
                            <TableHead className="font-medium text-right">CY</TableHead>
                            <TableHead className="font-medium text-right">Change</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {topGains.slice(0, 10).map((school, idx) => (
                            <TableRow key={idx} className="hover:bg-slate-50">
                              <TableCell className="max-w-xs truncate text-sm">
                                <SchoolLink udiseCode={school.udise_code}>{school.school_name}</SchoolLink>
                              </TableCell>
                              <TableCell className="text-sm">
                                <BlockLink blockCode={school.block_code}>{school.block_name}</BlockLink>
                              </TableCell>
                              <TableCell className="text-right tabular-nums">{school.teachers_py}</TableCell>
                              <TableCell className="text-right tabular-nums">{school.teachers_cy}</TableCell>
                              <TableCell className="text-right">
                                <Badge className="bg-emerald-100 text-emerald-700">+{school.change}</Badge>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  </CardContent>
                </Card>

                {/* Top Losses */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                      <ArrowDownRight className="w-5 h-5 text-red-500" />
                      Top 10 Schools - Highest Staffing Loss
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="overflow-x-auto">
                      <Table>
                        <TableHeader>
                          <TableRow className="bg-slate-50">
                            <TableHead className="font-medium">School</TableHead>
                            <TableHead className="font-medium">Block</TableHead>
                            <TableHead className="font-medium text-right">PY</TableHead>
                            <TableHead className="font-medium text-right">CY</TableHead>
                            <TableHead className="font-medium text-right">Change</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {topLosses.slice(0, 10).map((school, idx) => (
                            <TableRow key={idx} className="hover:bg-slate-50">
                              <TableCell className="max-w-xs truncate text-sm">
                                <SchoolLink udiseCode={school.udise_code}>{school.school_name}</SchoolLink>
                              </TableCell>
                              <TableCell className="text-sm">
                                <BlockLink blockCode={school.block_code}>{school.block_name}</BlockLink>
                              </TableCell>
                              <TableCell className="text-right tabular-nums">{school.teachers_py}</TableCell>
                              <TableCell className="text-right tabular-nums">{school.teachers_cy}</TableCell>
                              <TableCell className="text-right">
                                <Badge className="bg-red-100 text-red-700">{school.change}</Badge>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Block Performance Table */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Block-wise Detailed Metrics
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow className="bg-slate-50">
                          <TableHead className="font-medium">Block</TableHead>
                          <TableHead className="font-medium text-right">Schools</TableHead>
                          <TableHead className="font-medium text-right">Teachers CY</TableHead>
                          <TableHead className="font-medium text-right">Growth %</TableHead>
                          <TableHead className="font-medium text-right">CTET %</TableHead>
                          <TableHead className="font-medium text-right">CWSN %</TableHead>
                          <TableHead className="font-medium text-right">Digital %</TableHead>
                          <TableHead className="font-medium text-right">TQI</TableHead>
                          <TableHead className="font-medium text-right">TRI</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {blockData.slice(0, 15).map((block, idx) => (
                          <TableRow key={idx} className="hover:bg-slate-50">
                            <TableCell className="font-medium">
                              <BlockLink blockCode={block.block_code}>{block.block_name}</BlockLink>
                            </TableCell>
                            <TableCell className="text-right tabular-nums">{block.total_schools}</TableCell>
                            <TableCell className="text-right tabular-nums">{block.teachers_cy?.toLocaleString()}</TableCell>
                            <TableCell className="text-right">
                              <Badge className={block.teacher_growth_pct >= 0 ? "bg-emerald-100 text-emerald-700" : "bg-red-100 text-red-700"}>
                                {block.teacher_growth_pct >= 0 ? '+' : ''}{block.teacher_growth_pct}%
                              </Badge>
                            </TableCell>
                            <TableCell className="text-right tabular-nums">{block.ctet_pct}%</TableCell>
                            <TableCell className="text-right tabular-nums">{block.cwsn_pct}%</TableCell>
                            <TableCell className="text-right tabular-nums">{block.computer_pct}%</TableCell>
                            <TableCell className="text-right">
                              <Badge className="bg-blue-100 text-blue-700">{block.quality_index}</Badge>
                            </TableCell>
                            <TableCell className="text-right">
                              <Badge className={block.risk_index > 10 ? "bg-red-100 text-red-700" : "bg-amber-100 text-amber-700"}>
                                {block.risk_index}
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
                title="Teacher Analytics Insights"
                description="AI insights, root cause signals, recommendations, and priority actions for teacher analytics."
                generate={buildInsights}
              />
            </TabsContent>
          </Tabs>
        </>
      )}
    </div>
  );
};

export default TeacherDashboard;
