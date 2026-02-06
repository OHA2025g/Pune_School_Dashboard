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
  FileText, 
  AlertTriangle, 
  RefreshCw,
  Upload,
  School,
  TrendingUp,
  CheckCircle2,
  XCircle,
  AlertCircle,
  BarChart3,
  Activity,
  Target,
  Users,
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
  Legend
} from "recharts";

import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;

// KPI Card Component
const KPICard = ({ label, value, suffix = "", icon: Icon, color = "blue", description }) => {
  const colors = {
    blue: "bg-blue-50 text-blue-600",
    green: "bg-emerald-50 text-emerald-600",
    red: "bg-red-50 text-red-600",
    amber: "bg-amber-50 text-amber-600",
    purple: "bg-purple-50 text-purple-600",
    cyan: "bg-cyan-50 text-cyan-600",
  };

  return (
    <Card className="border-slate-200">
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

// Quality Indicator
const QualityIndicator = ({ value, label }) => {
  const getColor = (val) => {
    if (val >= 90) return { bg: "bg-emerald-500", text: "text-emerald-700" };
    if (val >= 70) return { bg: "bg-amber-500", text: "text-amber-700" };
    return { bg: "bg-red-500", text: "text-red-700" };
  };
  
  const { bg, text } = getColor(value);
  
  return (
    <div className="text-center">
      <div className="relative w-20 h-20 mx-auto">
        <svg className="w-full h-full transform -rotate-90">
          <circle cx="40" cy="40" r="35" stroke="#e2e8f0" strokeWidth="6" fill="none" />
          <circle 
            cx="40" cy="40" r="35" 
            className={bg.replace('bg-', 'stroke-')}
            strokeWidth="6" 
            fill="none"
            strokeDasharray={`${(value / 100) * 220} 220`}
            strokeLinecap="round"
          />
        </svg>
        <div className="absolute inset-0 flex items-center justify-center">
          <span className={`text-lg font-bold ${text}`}>{value}%</span>
        </div>
      </div>
      <p className="text-sm text-slate-600 mt-2">{label}</p>
    </div>
  );
};

const DropboxDashboard = () => {
  const { scope } = useScope();
  const [loading, setLoading] = useState(true);
  const [importing, setImporting] = useState(false);
  const [overview, setOverview] = useState(null);
  const [categoryData, setCategoryData] = useState([]);
  const [blockData, setBlockData] = useState([]);
  const [topSchools, setTopSchools] = useState([]);
  const [dataQuality, setDataQuality] = useState(null);
  const [transitionData, setTransitionData] = useState([]);
  const [dropoutHotspots, setDropoutHotspots] = useState([]);
  const [activeTab, setActiveTab] = useState("overview");

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [
        overviewRes,
        categoryRes,
        blockRes,
        topRes,
        qualityRes,
        transitionRes,
        hotspotRes
      ] = await Promise.all([
        axios.get(`${API}/dropbox/overview`),
        axios.get(`${API}/dropbox/category-distribution`),
        axios.get(`${API}/dropbox/block-wise`),
        axios.get(`${API}/dropbox/top-schools`),
        axios.get(`${API}/dropbox/data-quality`),
        axios.get(`${API}/dropbox/transition-analysis`),
        axios.get(`${API}/dropbox/dropout-hotspots`)
      ]);
      
      setOverview(overviewRes.data);
      setCategoryData(categoryRes.data);
      setBlockData(blockRes.data);
      setTopSchools(topRes.data);
      setDataQuality(qualityRes.data);
      setTransitionData(transitionRes.data);
      setDropoutHotspots(hotspotRes.data);
    } catch (error) {
      console.error("Error fetching Dropbox data:", error);
      toast.error("Failed to load dropbox remarks data");
    } finally {
      setLoading(false);
    }
  }, [scope.version]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handleImport = async () => {
    const url = "https://customer-assets.emergentagent.com/job_7d6cbc1e-b567-4fbc-af84-06fa5107bbd4/artifacts/lr06jctk_5.%20Dropbox%20Remarks%20Statistics%20-%20School%20Wise%20-%20Real%20Time%20%28State%29%20MAHARASHTRA.xlsx";
    
    setImporting(true);
    try {
      await axios.post(`${API}/dropbox/import?url=${encodeURIComponent(url)}`);
      toast.success("Dropbox Remarks data import started!");
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
        "- No dropbox remarks data available for the current filters.",
        "",
        "## Root Cause Signals",
        "- Missing or incomplete records in the selected scope.",
        "",
        "## Recommendations",
        "- Refresh data or broaden filters to include valid records.",
        "",
        "## Priority Action Items",
        "- Validate data import for dropbox remarks.",
      ].join("\n");
    }

    const fmt = (v) => (typeof v === "number" ? v.toLocaleString("en-IN") : v ?? 0);
    const blocks = Array.isArray(blockData) ? blockData : [];
    const worstErrors = [...blocks].sort((a, b) => (b.error_pct || 0) - (a.error_pct || 0)).slice(0, 3);

    return [
      "## Insights",
      `- Remarks coverage: **${fmt(overview.schools_with_remarks)}** schools reporting, avg **${overview.avg_remarks_per_school}** per school.`,
      `- Dropout remarks: **${fmt(overview.dropout)}** (${overview.dropout_pct}% of remarks).`,
      `- Data accuracy: **${overview.data_accuracy}%** (risk index **${overview.data_risk_index}%**).`,
      worstErrors.length ? `- Highest error blocks: **${worstErrors.map((b) => b.block_name).join(", ")}**.` : "- Highest error blocks: unavailable.",
      "",
      "## Root Cause Signals",
      "- High error % indicates inconsistent remark capture or validation gaps.",
      "- Elevated dropout remarks point to attendance/retention issues.",
      "",
      "## Recommendations",
      "- Standardize remark categories and train data entry staff.",
      "- Review dropout clusters and follow up with schools weekly.",
      "",
      "## Priority Action Items",
      worstErrors.length ? `- Week 1: reduce error % in **${worstErrors.map((b) => b.block_name).join(", ")}** (target <5%).` : "- Week 1: reduce error % in highest-error blocks (target <5%).",
      `- Week 2: reconcile **${fmt(overview.dropout)}** dropout remarks with registers and update statuses.`,
      "- Week 3–4: publish corrected remark dashboards and enforce SOP.",
    ].join("\n");
  };

  return (
    <div className="space-y-6 animate-fade-in" data-testid="dropbox-dashboard">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight" style={{ fontFamily: 'Manrope' }}>
            Dropbox Remarks Analytics
          </h1>
          <p className="text-slate-500 mt-1">School-wise Remarks Statistics • Real-Time • Pune District</p>
        </div>
        <div className="flex items-center gap-3">
          <Button 
            variant="outline" 
            size="sm" 
            onClick={handleImport}
            disabled={importing}
            data-testid="import-dropbox-btn"
          >
            {importing ? <RefreshCw className="w-4 h-4 mr-2 animate-spin" /> : <Upload className="w-4 h-4 mr-2" />}
            {importing ? "Importing..." : "Import Data"}
          </Button>
          <ExportPanel dashboardName="dropbox" dashboardTitle="Dropbox Remarks" />
          <Button variant="outline" size="sm" onClick={fetchData} data-testid="refresh-btn">
            <RefreshCw className="w-4 h-4 mr-2" />
            Refresh
          </Button>
        </div>
      </div>

      {!hasData ? (
        <Card className="border-slate-200">
          <CardContent className="py-12 text-center">
            <FileText className="w-16 h-16 mx-auto text-slate-300 mb-4" />
            <h3 className="text-xl font-semibold text-slate-700 mb-2">No Dropbox Remarks Data Available</h3>
            <p className="text-slate-500 mb-4">Click "Import Data" to load the Dropbox Remarks Excel file</p>
            <Button onClick={handleImport} disabled={importing} data-testid="import-dropbox-empty-btn">
              {importing ? "Importing..." : "Import Dropbox Data"}
            </Button>
          </CardContent>
        </Card>
      ) : (
        <>
          {/* Dashboard Tabs */}
          <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
            <TabsList className="bg-slate-100">
              <TabsTrigger value="overview" className="flex items-center gap-2" data-testid="tab-overview">
                <BarChart3 className="w-4 h-4" />
                State Overview
              </TabsTrigger>
              <TabsTrigger value="blocks" className="flex items-center gap-2" data-testid="tab-blocks">
                <Target className="w-4 h-4" />
                Block Performance
              </TabsTrigger>
              <TabsTrigger value="schools" className="flex items-center gap-2" data-testid="tab-schools">
                <School className="w-4 h-4" />
                School Deep Dive
              </TabsTrigger>
              <TabsTrigger value="governance" className="flex items-center gap-2" data-testid="tab-governance">
                <AlertTriangle className="w-4 h-4" />
                Risk & Governance
              </TabsTrigger>
              <TabsTrigger value="insights" className="flex items-center gap-2" data-testid="tab-insights">
                <Brain className="w-4 h-4" />
                Insights
              </TabsTrigger>
            </TabsList>

            {/* DASHBOARD 1: STATE OVERVIEW */}
            <TabsContent value="overview" className="space-y-6">
              {/* Top KPI Strip */}
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
                <KPICard
                  label="Total Schools"
                  value={overview.total_schools}
                  icon={School}
                  color="blue"
                />
                <KPICard
                  label="Schools Reporting"
                  value={overview.schools_with_remarks}
                  icon={CheckCircle2}
                  color="green"
                  description={`${overview.reporting_rate}% penetration`}
                />
                <KPICard
                  label="Total Remarks"
                  value={overview.total_remarks}
                  icon={FileText}
                  color="purple"
                />
                <KPICard
                  label="Avg/School"
                  value={overview.avg_remarks_per_school}
                  icon={Activity}
                  color="cyan"
                />
                <KPICard
                  label="Dropout Count"
                  value={overview.dropout}
                  icon={AlertTriangle}
                  color="red"
                  description={`${overview.dropout_pct}% of remarks`}
                />
                <KPICard
                  label="Data Accuracy"
                  value={overview.data_accuracy}
                  suffix="%"
                  icon={CheckCircle2}
                  color={overview.data_accuracy >= 90 ? "green" : "amber"}
                />
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Category Distribution */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Remark Category Distribution
                    </CardTitle>
                    <CardDescription>Volume by remark type</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-80">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={categoryData.filter(c => c.value > 0)} layout="vertical" margin={{ left: 100 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis type="number" />
                          <YAxis dataKey="name" type="category" width={90} tick={{ fontSize: 11 }} />
                          <Tooltip content={<CustomTooltip />} />
                          <Bar dataKey="value" name="Count" radius={[0, 4, 4, 0]}>
                            {categoryData.filter(c => c.value > 0).map((entry, index) => (
                              <Cell key={`cell-${index}`} fill={entry.color} />
                            ))}
                          </Bar>
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>

                {/* Data Quality Donut */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Data Quality Distribution
                    </CardTitle>
                    <CardDescription>Valid vs Error vs Pending</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-80">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={dataQuality?.quality_distribution || []}
                            cx="50%"
                            cy="50%"
                            innerRadius={60}
                            outerRadius={100}
                            paddingAngle={2}
                            dataKey="value"
                            label={({ name, value }) => `${value.toLocaleString()}`}
                          >
                            {(dataQuality?.quality_distribution || []).map((entry, index) => (
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
              </div>

              {/* Top Blocks */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Top 10 Blocks by Operational Load
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="h-72">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={blockData.slice(0, 10)}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="block_name" tick={{ fontSize: 10 }} angle={-45} textAnchor="end" height={60} />
                        <YAxis />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="total_remarks" name="Total Remarks" fill="#3b82f6" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* DASHBOARD 2: BLOCK PERFORMANCE */}
            <TabsContent value="blocks" className="space-y-6">
              {/* Block Rankings */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Block-wise Performance Ranking
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow className="bg-slate-50">
                          <TableHead className="font-medium">Block</TableHead>
                          <TableHead className="font-medium text-right">Schools</TableHead>
                          <TableHead className="font-medium text-right">Total Remarks</TableHead>
                          <TableHead className="font-medium text-right">Avg/School</TableHead>
                          <TableHead className="font-medium text-right">Dropout</TableHead>
                          <TableHead className="font-medium text-right">Class 12 Passed</TableHead>
                          <TableHead className="font-medium text-right">Error %</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {blockData.slice(0, 15).map((block, idx) => (
                          <TableRow key={idx} className="hover:bg-slate-50">
                            <TableCell className="font-medium">
                              <BlockLink blockCode={block.block_code}>{block.block_name}</BlockLink>
                            </TableCell>
                            <TableCell className="text-right tabular-nums">{block.total_schools}</TableCell>
                            <TableCell className="text-right tabular-nums">{block.total_remarks?.toLocaleString()}</TableCell>
                            <TableCell className="text-right tabular-nums">{block.avg_remarks_per_school}</TableCell>
                            <TableCell className="text-right">
                              <Badge className={block.dropout > 50 ? "bg-red-100 text-red-700" : "bg-slate-100 text-slate-700"}>
                                {block.dropout}
                              </Badge>
                            </TableCell>
                            <TableCell className="text-right tabular-nums text-emerald-600 font-medium">{block.class12_passed?.toLocaleString()}</TableCell>
                            <TableCell className="text-right">
                              <Badge className={block.error_pct > 5 ? "bg-red-100 text-red-700" : "bg-emerald-100 text-emerald-700"}>
                                {block.error_pct}%
                              </Badge>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>

              {/* Block Category Mix */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Block-wise Category Mix
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={blockData.slice(0, 12)} margin={{ left: 10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="block_name" tick={{ fontSize: 9 }} angle={-45} textAnchor="end" height={70} />
                        <YAxis />
                        <Tooltip content={<CustomTooltip />} />
                        <Legend />
                        <Bar dataKey="class12_passed" name="Class 12 Passed" stackId="a" fill="#10b981" />
                        <Bar dataKey="migrated_domestic" name="Migration" stackId="a" fill="#3b82f6" />
                        <Bar dataKey="iti_polytechnic" name="ITI/Poly" stackId="a" fill="#8b5cf6" />
                        <Bar dataKey="dropout" name="Dropout" stackId="a" fill="#ef4444" />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* DASHBOARD 3: SCHOOL DEEP DIVE */}
            <TabsContent value="schools" className="space-y-6">
              {/* Top Schools Table */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Top 20 Schools by Remarks Volume
                  </CardTitle>
                  <CardDescription>Schools with highest operational load</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow className="bg-slate-50">
                          <TableHead className="font-medium">#</TableHead>
                          <TableHead className="font-medium">School Name</TableHead>
                          <TableHead className="font-medium">Block</TableHead>
                          <TableHead className="font-medium text-right">Total</TableHead>
                          <TableHead className="font-medium text-right">Dropout</TableHead>
                          <TableHead className="font-medium text-right">Class 12</TableHead>
                          <TableHead className="font-medium text-right">Errors</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {topSchools.map((school, idx) => (
                          <TableRow key={idx} className="hover:bg-slate-50">
                            <TableCell className="text-slate-500">{idx + 1}</TableCell>
                            <TableCell className="font-medium max-w-xs truncate">
                              <SchoolLink udiseCode={school.udise_code}>{school.school_name}</SchoolLink>
                            </TableCell>
                            <TableCell>
                              <BlockLink blockCode={school.block_code}>{school.block_name}</BlockLink>
                            </TableCell>
                            <TableCell className="text-right tabular-nums font-bold">{school.total_remarks?.toLocaleString()}</TableCell>
                            <TableCell className="text-right">
                              {school.dropout > 0 ? (
                                <Badge className="bg-red-100 text-red-700">{school.dropout}</Badge>
                              ) : (
                                <span className="text-slate-400">0</span>
                              )}
                            </TableCell>
                            <TableCell className="text-right text-emerald-600 font-medium">{school.class12_passed?.toLocaleString()}</TableCell>
                            <TableCell className="text-right">
                              {school.wrong_entry > 0 ? (
                                <Badge className="bg-amber-100 text-amber-700">{school.wrong_entry}</Badge>
                              ) : (
                                <span className="text-slate-400">0</span>
                              )}
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>

              {/* Student Transition Flow */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Student Transition Analysis
                  </CardTitle>
                  <CardDescription>Where students are transitioning to</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={transitionData.filter(t => t.value > 0)} layout="vertical" margin={{ left: 120 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis type="number" />
                        <YAxis dataKey="name" type="category" width={110} tick={{ fontSize: 11 }} />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="value" name="Students" radius={[0, 4, 4, 0]}>
                          {transitionData.filter(t => t.value > 0).map((entry, index) => (
                            <Cell key={`cell-${index}`} fill={entry.color} />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* DASHBOARD 4: RISK & GOVERNANCE */}
            <TabsContent value="governance" className="space-y-6">
              {/* Data Quality Metrics */}
              <div className="grid grid-cols-1 md:grid-cols-4 gap-6">
                <Card className="border-slate-200">
                  <CardContent className="p-6">
                    <QualityIndicator value={dataQuality?.valid_pct || 0} label="Valid Data" />
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-red-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Wrong Entry/Duplicate</p>
                    <p className="text-2xl font-bold text-red-600">{dataQuality?.error_count?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">{dataQuality?.error_pct}% of total</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-amber-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Pending Import</p>
                    <p className="text-2xl font-bold text-amber-600">{dataQuality?.pending_count?.toLocaleString()}</p>
                    <p className="text-sm text-slate-600">{dataQuality?.pending_pct}% of total</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-slate-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Data Risk Index</p>
                    <p className="text-2xl font-bold text-slate-700">{overview.data_risk_index}%</p>
                    <p className="text-sm text-slate-600">Error + Pending</p>
                  </CardContent>
                </Card>
              </div>

              {/* Dropout Hotspots */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                    <AlertTriangle className="w-5 h-5 text-red-500" />
                    Dropout Hotspots (Block-wise)
                  </CardTitle>
                  <CardDescription>Blocks requiring immediate intervention</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    {dropoutHotspots.slice(0, 10).map((block, idx) => (
                      <div key={idx} className="flex items-center justify-between p-3 bg-red-50 rounded-lg">
                        <div className="flex items-center gap-3">
                          <span className="w-6 h-6 flex items-center justify-center bg-red-100 text-red-700 text-xs font-bold rounded-full">
                            {idx + 1}
                          </span>
                          <div>
                            <p className="font-medium text-slate-900">{block.block_name}</p>
                            <p className="text-xs text-slate-500">{block.total_schools} schools • {block.dropout_density} dropout/school</p>
                          </div>
                        </div>
                        <div className="text-right">
                          <Badge className="bg-red-100 text-red-700">{block.dropout} dropouts</Badge>
                          {block.due_to_death > 0 && (
                            <p className="text-xs text-slate-500 mt-1">{block.due_to_death} deaths</p>
                          )}
                        </div>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>

              {/* Summary Cards */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <Card className="border-slate-200 bg-red-50">
                  <CardContent className="p-6 text-center">
                    <AlertTriangle className="w-10 h-10 mx-auto text-red-500 mb-3" />
                    <p className="text-3xl font-bold text-red-700" style={{ fontFamily: 'Manrope' }}>{overview.dropout}</p>
                    <p className="text-sm text-red-600">Total Dropouts</p>
                    <p className="text-xs text-slate-500 mt-2">Requires retention intervention</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 bg-emerald-50">
                  <CardContent className="p-6 text-center">
                    <CheckCircle2 className="w-10 h-10 mx-auto text-emerald-500 mb-3" />
                    <p className="text-3xl font-bold text-emerald-700" style={{ fontFamily: 'Manrope' }}>{overview.class12_passed?.toLocaleString()}</p>
                    <p className="text-sm text-emerald-600">Class 12 Passed</p>
                    <p className="text-xs text-slate-500 mt-2">Academic completion</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 bg-purple-50">
                  <CardContent className="p-6 text-center">
                    <TrendingUp className="w-10 h-10 mx-auto text-purple-500 mb-3" />
                    <p className="text-3xl font-bold text-purple-700" style={{ fontFamily: 'Manrope' }}>{overview.iti_polytechnic?.toLocaleString()}</p>
                    <p className="text-sm text-purple-600">Skill Education Shift</p>
                    <p className="text-xs text-slate-500 mt-2">ITI/Polytechnic transitions</p>
                  </CardContent>
                </Card>
              </div>
            </TabsContent>
            <TabsContent value="insights" className="space-y-6">
              <AiInsightsTab
                title="Dropbox Remarks Insights"
                description="AI insights, root cause signals, recommendations, and priority actions for dropbox remarks."
                generate={buildInsights}
              />
            </TabsContent>
          </Tabs>
        </>
      )}
    </div>
  );
};

export default DropboxDashboard;
