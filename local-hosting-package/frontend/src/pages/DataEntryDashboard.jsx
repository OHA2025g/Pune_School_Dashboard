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
  FileCheck, 
  AlertTriangle, 
  RefreshCw,
  Upload,
  School,
  TrendingUp,
  CheckCircle2,
  XCircle,
  Clock,
  BarChart3,
  Activity,
  Target,
  Users,
  Repeat,
  ShieldCheck,
  AlertCircle,
  Gauge,
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
  Line
} from "recharts";

import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;

// KPI Card Component
const KPICard = ({ label, value, suffix = "", icon: Icon, color = "blue", description, trend }) => {
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
            {description && <p className="text-xs text-slate-400 mt-1">{description}</p>}
            {trend && (
              <p className={`text-xs mt-1 ${trend >= 0 ? 'text-emerald-600' : 'text-red-600'}`}>
                {trend >= 0 ? '↑' : '↓'} {Math.abs(trend)}% from PY
              </p>
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

// Gauge Component
const GaugeChart = ({ value, label, color = "#10b981" }) => {
  const getColor = (val) => {
    if (val >= 99) return "#10b981";
    if (val >= 95) return "#f59e0b";
    return "#ef4444";
  };
  
  const actualColor = getColor(value);
  const circumference = 2 * Math.PI * 45;
  const strokeDasharray = `${(value / 100) * circumference} ${circumference}`;
  
  return (
    <div className="flex flex-col items-center">
      <div className="relative w-28 h-28">
        <svg className="w-full h-full transform -rotate-90">
          <circle cx="56" cy="56" r="45" stroke="#e2e8f0" strokeWidth="10" fill="none" />
          <circle 
            cx="56" cy="56" r="45" 
            stroke={actualColor}
            strokeWidth="10" 
            fill="none"
            strokeDasharray={strokeDasharray}
            strokeLinecap="round"
          />
        </svg>
        <div className="absolute inset-0 flex items-center justify-center">
          <span className="text-xl font-bold" style={{ color: actualColor }}>{value}%</span>
        </div>
      </div>
      <p className="text-sm text-slate-600 mt-2 text-center">{label}</p>
    </div>
  );
};

// RAG Badge
const RAGBadge = ({ value, thresholds = { green: 99, amber: 95 } }) => {
  let color = "bg-emerald-100 text-emerald-700";
  if (value < thresholds.amber) color = "bg-red-100 text-red-700";
  else if (value < thresholds.green) color = "bg-amber-100 text-amber-700";
  
  return <Badge className={color}>{value}%</Badge>;
};

const DataEntryDashboard = () => {
  const { scope } = useScope();
  const [loading, setLoading] = useState(true);
  const [importing, setImporting] = useState(false);
  const [overview, setOverview] = useState(null);
  const [blockData, setBlockData] = useState([]);
  const [completionBands, setCompletionBands] = useState(null);
  const [certificationData, setCertificationData] = useState(null);
  const [repeaterData, setRepeaterData] = useState([]);
  const [criticalSchools, setCriticalSchools] = useState([]);
  const [highRepeaterSchools, setHighRepeaterSchools] = useState([]);
  const [dataQuality, setDataQuality] = useState(null);
  const [topBottomBlocks, setTopBottomBlocks] = useState(null);
  const [activeTab, setActiveTab] = useState("executive");

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [
        overviewRes,
        blockRes,
        bandsRes,
        certRes,
        repeaterRes,
        criticalRes,
        highRepeaterRes,
        qualityRes,
        topBottomRes
      ] = await Promise.all([
        axios.get(`${API}/data-entry/overview`),
        axios.get(`${API}/data-entry/block-wise`),
        axios.get(`${API}/data-entry/school-completion-bands`),
        axios.get(`${API}/data-entry/certification-status`),
        axios.get(`${API}/data-entry/repeater-analysis`),
        axios.get(`${API}/data-entry/critical-schools`),
        axios.get(`${API}/data-entry/high-repeater-schools`),
        axios.get(`${API}/data-entry/data-quality`),
        axios.get(`${API}/data-entry/top-bottom-blocks`)
      ]);
      
      setOverview(overviewRes.data);
      setBlockData(blockRes.data);
      setCompletionBands(bandsRes.data);
      setCertificationData(certRes.data);
      setRepeaterData(repeaterRes.data);
      setCriticalSchools(criticalRes.data);
      setHighRepeaterSchools(highRepeaterRes.data);
      setDataQuality(qualityRes.data);
      setTopBottomBlocks(topBottomRes.data);
    } catch (error) {
      console.error("Error fetching Data Entry data:", error);
      toast.error("Failed to load data entry status data");
    } finally {
      setLoading(false);
    }
  }, [scope.version]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handleImport = async () => {
    setImporting(true);
    try {
      await axios.post(`${API}/data-entry/import`);
      toast.success("Data Entry Status import started!");
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

  // Prepare chart data
  const completionStatusData = hasData ? [
    { name: "Completed", value: overview.total_completed, color: "#10b981" },
    { name: "In Progress", value: overview.total_in_progress, color: "#f59e0b" },
    { name: "Not Started", value: overview.total_not_started, color: "#ef4444" }
  ].filter(d => d.value > 0) : [];

  const buildInsights = () => {
    if (!overview) {
      return [
        "## Insights",
        "- No data entry status data available for the current filters.",
        "",
        "## Root Cause Signals",
        "- Missing or incomplete records in the selected scope.",
        "",
        "## Recommendations",
        "- Refresh data or broaden filters to include valid records.",
        "",
        "## Priority Action Items",
        "- Validate data import for data entry status.",
      ].join("\n");
    }

    const fmt = (v) => (typeof v === "number" ? v.toLocaleString("en-IN") : v ?? 0);
    const pct = (v) => (typeof v === "number" ? `${v}%` : "0%");

    return [
      "## Insights",
      `- Completion rate: **${pct(overview.completion_rate)}** across **${fmt(overview.total_students)}** students.`,
      `- Pending students: **${fmt(overview.pending_students)}** (${pct(overview.pending_rate)}).`,
      `- Certification rate: **${pct(overview.certification_rate)}** with **${fmt(overview.certified_schools)}** schools certified.`,
      `- Critical schools (<95% completion): **${fmt(criticalSchools.length)}**.`,
      "",
      "## Root Cause Signals",
      "- High pending and not-started counts indicate backlog in data entry workflows.",
      "- Low certification rate points to incomplete verification/approval steps.",
      "",
      "## Recommendations",
      "- Prioritize completion drives for critical and high-repeater schools.",
      "- Enforce weekly review of pending entries at block level.",
      "",
      "## Priority Action Items",
      `- Week 1: clear **${fmt(overview.total_not_started)}** not-started schools and assign block coordinators.`,
      `- Week 2: fix **${fmt(criticalSchools.length)}** critical schools to reach 95% completion.`,
      `- Week 3–4: raise certification from **${pct(overview.certification_rate)}** to target >= 90%.`,
    ].join("\n");
  };

  const schoolBandData = completionBands ? [
    { name: "100%", value: completionBands["100%"], color: "#10b981" },
    { name: "95-99%", value: completionBands["95-99%"], color: "#3b82f6" },
    { name: "90-95%", value: completionBands["90-95%"], color: "#f59e0b" },
    { name: "<90%", value: completionBands["<90%"], color: "#ef4444" }
  ] : [];

  return (
    <div className="space-y-6 animate-fade-in" data-testid="data-entry-dashboard">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight" style={{ fontFamily: 'Manrope' }}>
            Data Entry Status Dashboard
          </h1>
          <p className="text-slate-500 mt-1">School-wise Entry Monitoring • Real-Time • Pune District • 2025-26</p>
        </div>
        <div className="flex items-center gap-3">
          <Button 
            variant="outline" 
            size="sm" 
            onClick={handleImport}
            disabled={importing}
            data-testid="import-data-entry-btn"
          >
            {importing ? <RefreshCw className="w-4 h-4 mr-2 animate-spin" /> : <Upload className="w-4 h-4 mr-2" />}
            {importing ? "Importing..." : "Import Data"}
          </Button>
          <ExportPanel dashboardName="data-entry" dashboardTitle="Data Entry Status" />
          <Button variant="outline" size="sm" onClick={fetchData} data-testid="refresh-btn">
            <RefreshCw className="w-4 h-4 mr-2" />
            Refresh
          </Button>
        </div>
      </div>

      {!hasData ? (
        <Card className="border-slate-200">
          <CardContent className="py-12 text-center">
            <FileCheck className="w-16 h-16 mx-auto text-slate-300 mb-4" />
            <h3 className="text-xl font-semibold text-slate-700 mb-2">No Data Entry Status Data Available</h3>
            <p className="text-slate-500 mb-4">Click "Import Data" to load the Data Entry Status Excel file</p>
            <Button onClick={handleImport} disabled={importing} data-testid="import-data-entry-empty-btn">
              {importing ? "Importing..." : "Import Data Entry Status Data"}
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
                Executive View
              </TabsTrigger>
              <TabsTrigger value="blocks" className="flex items-center gap-2" data-testid="tab-blocks">
                <Target className="w-4 h-4" />
                Block Performance
              </TabsTrigger>
              <TabsTrigger value="schools" className="flex items-center gap-2" data-testid="tab-schools">
                <School className="w-4 h-4" />
                School Monitoring
              </TabsTrigger>
              <TabsTrigger value="quality" className="flex items-center gap-2" data-testid="tab-quality">
                <ShieldCheck className="w-4 h-4" />
                Quality & Audit
              </TabsTrigger>
              <TabsTrigger value="insights" className="flex items-center gap-2" data-testid="tab-insights">
                <Brain className="w-4 h-4" />
                Insights
              </TabsTrigger>
            </TabsList>

            {/* DASHBOARD 1: EXECUTIVE VIEW */}
            <TabsContent value="executive" className="space-y-6">
              {/* Top KPI Strip - Executive Scorecard */}
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
                <KPICard
                  label="Total Students"
                  value={overview.total_students}
                  icon={Users}
                  color="blue"
                  description={`${overview.total_schools} schools`}
                />
                <KPICard
                  label="Completion Rate"
                  value={overview.completion_rate}
                  suffix="%"
                  icon={CheckCircle2}
                  color={overview.completion_rate >= 99 ? "green" : "amber"}
                />
                <KPICard
                  label="Pending Students"
                  value={overview.pending_students}
                  icon={Clock}
                  color={overview.pending_students > 0 ? "amber" : "green"}
                  description={`${overview.pending_rate}% pending`}
                />
                <KPICard
                  label="Certified Schools"
                  value={overview.certification_rate}
                  suffix="%"
                  icon={ShieldCheck}
                  color={overview.certification_rate >= 90 ? "green" : "red"}
                  description={`${overview.certified_schools} of ${overview.total_schools}`}
                />
                <KPICard
                  label="Repeater Rate"
                  value={overview.repeater_rate}
                  suffix="%"
                  icon={Repeat}
                  color={overview.repeater_rate < 5 ? "green" : "red"}
                  description={`${overview.total_repeaters?.toLocaleString()} students`}
                />
              </div>

              {/* Secondary KPIs */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                  label="100% Complete Schools"
                  value={overview.full_completion_rate}
                  suffix="%"
                  icon={CheckCircle2}
                  color="green"
                  description={`${overview.full_completion_schools} schools`}
                />
                <KPICard
                  label="Avg Students/School"
                  value={overview.avg_students_per_school}
                  icon={School}
                  color="cyan"
                />
                <KPICard
                  label="Total Blocks"
                  value={overview.total_blocks}
                  icon={Target}
                  color="purple"
                />
                <KPICard
                  label="Non-Certified"
                  value={overview.non_certified_schools}
                  icon={XCircle}
                  color="red"
                  description="Needs action"
                />
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Completion Status Donut */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Entry Status Distribution
                    </CardTitle>
                    <CardDescription>Completed vs Pending Students</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-72">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={completionStatusData}
                            cx="50%"
                            cy="50%"
                            innerRadius={60}
                            outerRadius={100}
                            paddingAngle={2}
                            dataKey="value"
                            label={({ name, value }) => `${(value / 1000).toFixed(0)}K`}
                          >
                            {completionStatusData.map((entry, index) => (
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

                {/* School Completion Bands */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      School Completion Bands
                    </CardTitle>
                    <CardDescription>Distribution by completion %</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-72">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={schoolBandData} layout="vertical" margin={{ left: 60 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis type="number" />
                          <YAxis dataKey="name" type="category" />
                          <Tooltip content={<CustomTooltip />} />
                          <Bar dataKey="value" name="Schools" radius={[0, 4, 4, 0]}>
                            {schoolBandData.map((entry, index) => (
                              <Cell key={`cell-${index}`} fill={entry.color} />
                            ))}
                          </Bar>
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Gauges Row */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Key Performance Gauges
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="flex flex-wrap justify-around gap-6 py-4">
                    <GaugeChart value={overview.completion_rate} label="Data Entry Completion" />
                    <GaugeChart value={overview.full_completion_rate} label="Fully Completed Schools" />
                    <GaugeChart value={overview.certification_rate} label="School Certification" />
                    <GaugeChart value={100 - overview.repeater_rate} label="Non-Repeater Rate" />
                  </div>
                </CardContent>
              </Card>

              {/* Top/Bottom Blocks */}
              {topBottomBlocks && (
                <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                  <Card className="border-slate-200 border-l-4 border-l-emerald-500">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                        <TrendingUp className="w-5 h-5 text-emerald-500" />
                        Top Performing Blocks
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="space-y-3">
                        {topBottomBlocks.top_blocks?.map((block, idx) => (
                          <div key={idx} className="flex items-center justify-between p-2 bg-emerald-50 rounded-lg">
                            <div className="flex items-center gap-2">
                              <span className="w-6 h-6 flex items-center justify-center bg-emerald-100 text-emerald-700 text-xs font-bold rounded-full">
                                {idx + 1}
                              </span>
                              <span className="font-medium">{block.block_name}</span>
                            </div>
                            <Badge className="bg-emerald-100 text-emerald-700">{block.completion_rate}%</Badge>
                          </div>
                        ))}
                      </div>
                    </CardContent>
                  </Card>

                  <Card className="border-slate-200 border-l-4 border-l-red-500">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                        <AlertTriangle className="w-5 h-5 text-red-500" />
                        Blocks Needing Attention
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="space-y-3">
                        {topBottomBlocks.bottom_blocks?.map((block, idx) => (
                          <div key={idx} className="flex items-center justify-between p-2 bg-red-50 rounded-lg">
                            <div className="flex items-center gap-2">
                              <span className="w-6 h-6 flex items-center justify-center bg-red-100 text-red-700 text-xs font-bold rounded-full">
                                {idx + 1}
                              </span>
                              <span className="font-medium">{block.block_name}</span>
                            </div>
                            <Badge className="bg-red-100 text-red-700">{block.completion_rate}%</Badge>
                          </div>
                        ))}
                      </div>
                    </CardContent>
                  </Card>
                </div>
              )}
            </TabsContent>

            {/* DASHBOARD 2: BLOCK PERFORMANCE */}
            <TabsContent value="blocks" className="space-y-6">
              {/* Block Rankings Table */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Block-wise Performance Ranking
                  </CardTitle>
                  <CardDescription>All {overview.total_blocks} blocks sorted by student volume</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow className="bg-slate-50">
                          <TableHead className="font-medium">Rank</TableHead>
                          <TableHead className="font-medium">Block</TableHead>
                          <TableHead className="font-medium text-right">Schools</TableHead>
                          <TableHead className="font-medium text-right">Students</TableHead>
                          <TableHead className="font-medium text-right">Completed</TableHead>
                          <TableHead className="font-medium text-right">Pending</TableHead>
                          <TableHead className="font-medium text-right">Completion %</TableHead>
                          <TableHead className="font-medium text-right">Repeaters</TableHead>
                          <TableHead className="font-medium text-right">Certified</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {blockData.map((block) => (
                          <TableRow key={block.block_name} className="hover:bg-slate-50">
                            <TableCell className="text-slate-500">{block.rank}</TableCell>
                            <TableCell className="font-medium">{block.block_name}</TableCell>
                            <TableCell className="text-right tabular-nums">{block.total_schools}</TableCell>
                            <TableCell className="text-right tabular-nums">{block.total_students?.toLocaleString()}</TableCell>
                            <TableCell className="text-right tabular-nums text-emerald-600">{block.total_completed?.toLocaleString()}</TableCell>
                            <TableCell className="text-right">
                              {block.pending_students > 0 ? (
                                <Badge className="bg-amber-100 text-amber-700">{block.pending_students}</Badge>
                              ) : (
                                <span className="text-slate-400">0</span>
                              )}
                            </TableCell>
                            <TableCell className="text-right">
                              <RAGBadge value={block.completion_pct} />
                            </TableCell>
                            <TableCell className="text-right tabular-nums">
                              {block.total_repeaters > 0 ? (
                                <span className="text-red-600">{block.total_repeaters?.toLocaleString()}</span>
                              ) : (
                                <span className="text-slate-400">0</span>
                              )}
                            </TableCell>
                            <TableCell className="text-right">
                              <Badge className={block.certification_rate >= 10 ? "bg-emerald-100 text-emerald-700" : "bg-red-100 text-red-700"}>
                                {block.certification_rate}%
                              </Badge>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>

              {/* Block Completion Chart */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Block-wise Pending Students
                  </CardTitle>
                  <CardDescription>Blocks with highest pending load</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart 
                        data={blockData.filter(b => b.pending_students > 0).sort((a, b) => b.pending_students - a.pending_students).slice(0, 15)}
                        margin={{ left: 10 }}
                      >
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="block_name" tick={{ fontSize: 10 }} angle={-45} textAnchor="end" height={70} />
                        <YAxis />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="pending_students" name="Pending Students" fill="#f59e0b" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>

              {/* Repeater Analysis by Block */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Block-wise Repeater Analysis
                  </CardTitle>
                  <CardDescription>Repeater distribution across blocks</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart 
                        data={repeaterData.slice(0, 15)}
                        margin={{ left: 10 }}
                      >
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="block_name" tick={{ fontSize: 10 }} angle={-45} textAnchor="end" height={70} />
                        <YAxis />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="total_repeaters" name="Repeaters" fill="#ef4444" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* DASHBOARD 3: SCHOOL MONITORING */}
            <TabsContent value="schools" className="space-y-6">
              {/* Critical Schools Table */}
              <Card className="border-slate-200 border-l-4 border-l-red-500">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                    <AlertTriangle className="w-5 h-5 text-red-500" />
                    Critical Schools (&lt;95% Completion)
                  </CardTitle>
                  <CardDescription>Schools requiring immediate intervention</CardDescription>
                </CardHeader>
                <CardContent>
                  {criticalSchools.length > 0 ? (
                    <div className="overflow-x-auto">
                      <Table>
                        <TableHeader>
                          <TableRow className="bg-red-50">
                            <TableHead className="font-medium">#</TableHead>
                            <TableHead className="font-medium">School Name</TableHead>
                            <TableHead className="font-medium">Block</TableHead>
                            <TableHead className="font-medium text-right">Total</TableHead>
                            <TableHead className="font-medium text-right">Completed</TableHead>
                            <TableHead className="font-medium text-right">In Progress</TableHead>
                            <TableHead className="font-medium text-right">Not Started</TableHead>
                            <TableHead className="font-medium text-right">Completion %</TableHead>
                            <TableHead className="font-medium">Certified</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {criticalSchools.map((school, idx) => (
                            <TableRow key={school.udise_code} className="hover:bg-red-50">
                              <TableCell className="text-slate-500">{idx + 1}</TableCell>
                              <TableCell className="font-medium max-w-xs truncate">
                                <SchoolLink udiseCode={school.udise_code}>{school.school_name}</SchoolLink>
                              </TableCell>
                              <TableCell>
                                <BlockLink blockCode={school.block_code}>{school.block_name}</BlockLink>
                              </TableCell>
                              <TableCell className="text-right tabular-nums">{school.total_students}</TableCell>
                              <TableCell className="text-right tabular-nums text-emerald-600">{school.total_completed}</TableCell>
                              <TableCell className="text-right">
                                {school.in_progress > 0 ? (
                                  <Badge className="bg-amber-100 text-amber-700">{school.in_progress}</Badge>
                                ) : "-"}
                              </TableCell>
                              <TableCell className="text-right">
                                {school.not_started > 0 ? (
                                  <Badge className="bg-red-100 text-red-700">{school.not_started}</Badge>
                                ) : "-"}
                              </TableCell>
                              <TableCell className="text-right">
                                <Badge className="bg-red-100 text-red-700">{school.completion_pct}%</Badge>
                              </TableCell>
                              <TableCell>
                                {school.certified === "Yes" ? (
                                  <CheckCircle2 className="w-4 h-4 text-emerald-500" />
                                ) : (
                                  <XCircle className="w-4 h-4 text-red-500" />
                                )}
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  ) : (
                    <div className="text-center py-8 text-emerald-600">
                      <CheckCircle2 className="w-12 h-12 mx-auto mb-2" />
                      <p className="font-medium">All schools above 95% completion!</p>
                    </div>
                  )}
                </CardContent>
              </Card>

              {/* High Repeater Schools */}
              <Card className="border-slate-200 border-l-4 border-l-amber-500">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                    <Repeat className="w-5 h-5 text-amber-500" />
                    High Repeater Schools (&gt;5% Rate)
                  </CardTitle>
                  <CardDescription>Schools with high repeater concentration</CardDescription>
                </CardHeader>
                <CardContent>
                  {highRepeaterSchools.length > 0 ? (
                    <div className="overflow-x-auto">
                      <Table>
                        <TableHeader>
                          <TableRow className="bg-amber-50">
                            <TableHead className="font-medium">#</TableHead>
                            <TableHead className="font-medium">School Name</TableHead>
                            <TableHead className="font-medium">Block</TableHead>
                            <TableHead className="font-medium text-right">Total Students</TableHead>
                            <TableHead className="font-medium text-right">Repeaters</TableHead>
                            <TableHead className="font-medium text-right">Repeater Rate</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {highRepeaterSchools.slice(0, 20).map((school, idx) => (
                            <TableRow key={school.udise_code} className="hover:bg-amber-50">
                              <TableCell className="text-slate-500">{idx + 1}</TableCell>
                              <TableCell className="font-medium max-w-xs truncate">
                                <SchoolLink udiseCode={school.udise_code}>{school.school_name}</SchoolLink>
                              </TableCell>
                              <TableCell>
                                <BlockLink blockCode={school.block_code}>{school.block_name}</BlockLink>
                              </TableCell>
                              <TableCell className="text-right tabular-nums">{school.total_students}</TableCell>
                              <TableCell className="text-right">
                                <Badge className="bg-red-100 text-red-700">{school.total_repeaters}</Badge>
                              </TableCell>
                              <TableCell className="text-right">
                                <Badge className="bg-amber-100 text-amber-700">{school.repeater_rate}%</Badge>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  ) : (
                    <div className="text-center py-8 text-emerald-600">
                      <CheckCircle2 className="w-12 h-12 mx-auto mb-2" />
                      <p className="font-medium">No schools with repeater rate above 5%!</p>
                    </div>
                  )}
                </CardContent>
              </Card>

              {/* Certification Status */}
              {certificationData && (
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      School Certification Status
                    </CardTitle>
                    <CardDescription>Certified vs Non-Certified distribution</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                      <div className="h-64">
                        <ResponsiveContainer width="100%" height="100%">
                          <PieChart>
                            <Pie
                              data={certificationData.distribution}
                              cx="50%"
                              cy="50%"
                              innerRadius={50}
                              outerRadius={80}
                              paddingAngle={2}
                              dataKey="value"
                              label={({ name, value }) => `${value.toLocaleString()}`}
                            >
                              {certificationData.distribution.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={entry.color} />
                              ))}
                            </Pie>
                            <Tooltip content={<CustomTooltip />} />
                            <Legend />
                          </PieChart>
                        </ResponsiveContainer>
                      </div>
                      <div className="space-y-4">
                        <div className="p-4 bg-emerald-50 rounded-lg">
                          <p className="text-sm text-slate-600">Certified Schools</p>
                          <p className="text-3xl font-bold text-emerald-700">{certificationData.certified?.schools?.toLocaleString()}</p>
                          <p className="text-sm text-slate-500">{certificationData.certified?.students?.toLocaleString()} students</p>
                        </div>
                        <div className="p-4 bg-red-50 rounded-lg">
                          <p className="text-sm text-slate-600">Non-Certified Schools</p>
                          <p className="text-3xl font-bold text-red-700">{certificationData.non_certified?.schools?.toLocaleString()}</p>
                          <p className="text-sm text-slate-500">{certificationData.non_certified?.students?.toLocaleString()} students</p>
                        </div>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              )}
            </TabsContent>

            {/* DASHBOARD 4: QUALITY & AUDIT */}
            <TabsContent value="quality" className="space-y-6">
              {/* Data Quality Metrics */}
              {dataQuality && (
                <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
                  <Card className="border-slate-200 border-l-4 border-l-emerald-500">
                    <CardContent className="p-4">
                      <p className="text-xs text-slate-500 uppercase">Data Consistency</p>
                      <p className="text-3xl font-bold text-emerald-600">{dataQuality.consistency_rate}%</p>
                      <p className="text-sm text-slate-600">{dataQuality.consistent_schools?.toLocaleString()} consistent schools</p>
                    </CardContent>
                  </Card>
                  <Card className="border-slate-200 border-l-4 border-l-red-500">
                    <CardContent className="p-4">
                      <p className="text-xs text-slate-500 uppercase">Data Mismatch</p>
                      <p className="text-3xl font-bold text-red-600">{dataQuality.mismatch_schools}</p>
                      <p className="text-sm text-slate-600">Schools with data issues</p>
                    </CardContent>
                  </Card>
                  <Card className="border-slate-200 border-l-4 border-l-amber-500">
                    <CardContent className="p-4">
                      <p className="text-xs text-slate-500 uppercase">Zero Entry Schools</p>
                      <p className="text-3xl font-bold text-amber-600">{dataQuality.zero_entry_schools}</p>
                      <p className="text-sm text-slate-600">No data entered</p>
                    </CardContent>
                  </Card>
                  <Card className="border-slate-200 border-l-4 border-l-blue-500">
                    <CardContent className="p-4">
                      <p className="text-xs text-slate-500 uppercase">Full Entry Schools</p>
                      <p className="text-3xl font-bold text-blue-600">{dataQuality.full_entry_schools?.toLocaleString()}</p>
                      <p className="text-sm text-slate-600">100% completed</p>
                    </CardContent>
                  </Card>
                </div>
              )}

              {/* Summary Cards */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <Card className="border-slate-200 bg-blue-50">
                  <CardContent className="p-6 text-center">
                    <Users className="w-10 h-10 mx-auto text-blue-500 mb-3" />
                    <p className="text-3xl font-bold text-blue-700" style={{ fontFamily: 'Manrope' }}>
                      {overview.total_students?.toLocaleString()}
                    </p>
                    <p className="text-sm text-blue-600">Total Students Covered</p>
                    <p className="text-xs text-slate-500 mt-2">Academic Year 2025-26</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 bg-emerald-50">
                  <CardContent className="p-6 text-center">
                    <CheckCircle2 className="w-10 h-10 mx-auto text-emerald-500 mb-3" />
                    <p className="text-3xl font-bold text-emerald-700" style={{ fontFamily: 'Manrope' }}>
                      {overview.total_completed?.toLocaleString()}
                    </p>
                    <p className="text-sm text-emerald-600">Data Entry Completed</p>
                    <p className="text-xs text-slate-500 mt-2">{overview.completion_rate}% completion rate</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 bg-red-50">
                  <CardContent className="p-6 text-center">
                    <AlertTriangle className="w-10 h-10 mx-auto text-red-500 mb-3" />
                    <p className="text-3xl font-bold text-red-700" style={{ fontFamily: 'Manrope' }}>
                      {overview.total_repeaters?.toLocaleString()}
                    </p>
                    <p className="text-sm text-red-600">Total Repeaters</p>
                    <p className="text-xs text-slate-500 mt-2">{overview.repeater_rate}% repeater rate</p>
                  </CardContent>
                </Card>
              </div>

              {/* Worst Block Alert */}
              {topBottomBlocks?.worst_block && (
                <Card className="border-slate-200 border-2 border-red-300 bg-red-50">
                  <CardContent className="p-6">
                    <div className="flex items-center gap-4">
                      <div className="p-3 bg-red-100 rounded-full">
                        <AlertCircle className="w-8 h-8 text-red-600" />
                      </div>
                      <div>
                        <h3 className="text-lg font-bold text-red-800">Escalation Alert: Worst Performing Block</h3>
                        <p className="text-red-700">
                          <strong>{topBottomBlocks.worst_block.block_name}</strong> has the lowest completion rate at{" "}
                          <strong>{topBottomBlocks.worst_block.completion_rate}%</strong>
                        </p>
                        <p className="text-sm text-red-600 mt-1">
                          {topBottomBlocks.worst_block.total_schools} schools • {topBottomBlocks.worst_block.total_students?.toLocaleString()} students
                        </p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              )}

              {/* Certification Gap */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Certification Coverage by Block
                  </CardTitle>
                  <CardDescription>% of certified schools per block</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart 
                        data={blockData.sort((a, b) => b.certification_rate - a.certification_rate).slice(0, 15)}
                        margin={{ left: 10 }}
                      >
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="block_name" tick={{ fontSize: 10 }} angle={-45} textAnchor="end" height={70} />
                        <YAxis domain={[0, 100]} />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="certification_rate" name="Certification %" radius={[4, 4, 0, 0]}>
                          {blockData.map((entry, index) => (
                            <Cell key={`cell-${index}`} fill={entry.certification_rate >= 10 ? "#10b981" : "#ef4444"} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>
            <TabsContent value="insights" className="space-y-6">
              <AiInsightsTab
                title="Data Entry Status Insights"
                description="AI insights, root cause signals, recommendations, and priority actions for data entry status."
                generate={buildInsights}
              />
            </TabsContent>
          </Tabs>
        </>
      )}
    </div>
  );
};

export default DataEntryDashboard;
