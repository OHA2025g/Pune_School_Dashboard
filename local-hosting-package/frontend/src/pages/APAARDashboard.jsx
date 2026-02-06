import { useState, useEffect, useCallback } from "react";
import axios from "axios";
import { useScope } from "@/context/ScopeContext";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Users, RefreshCw, Upload, School, BarChart3, Target, AlertTriangle, CheckCircle2, XCircle, Clock, TrendingUp, Brain } from "lucide-react";
import { toast } from "sonner";
import ExportPanel from "@/components/ExportPanel";
import AiInsightsTab from "@/components/AiInsightsTab";
import { BlockLink, SchoolLink } from "@/components/DrilldownLink";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell, PieChart, Pie, Legend, LineChart, Line } from "recharts";

import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;

const KPICard = ({ label, value, suffix = "", icon: Icon, color = "blue", description }) => {
  const colors = { blue: "bg-blue-50 text-blue-600", green: "bg-emerald-50 text-emerald-600", red: "bg-red-50 text-red-600", amber: "bg-amber-50 text-amber-600", purple: "bg-purple-50 text-purple-600" };
  return (
    <Card className="border-slate-200" data-testid={`kpi-${label.toLowerCase().replace(/\s+/g, '-')}`}>
      <CardContent className="p-4">
        <div className="flex items-start justify-between">
          <div className="flex-1">
            <p className="text-xs font-medium text-slate-500 uppercase tracking-wider">{label}</p>
            <div className="flex items-baseline gap-1 mt-1">
              <span className="text-2xl font-bold text-slate-900 tabular-nums" style={{ fontFamily: 'Manrope' }}>{typeof value === 'number' ? value.toLocaleString('en-IN') : value}</span>
              {suffix && <span className="text-lg text-slate-500">{suffix}</span>}
            </div>
            {description && <p className="text-xs text-slate-400 mt-1">{description}</p>}
          </div>
          {Icon && <div className={`p-2 rounded-lg ${colors[color]}`}><Icon className="w-5 h-5" strokeWidth={1.5} /></div>}
        </div>
      </CardContent>
    </Card>
  );
};

const GaugeChart = ({ value, label }) => {
  const getColor = (val) => val >= 90 ? "#10b981" : val >= 80 ? "#f59e0b" : "#ef4444";
  const actualColor = getColor(value);
  const circumference = 2 * Math.PI * 45;
  const strokeDasharray = `${(value / 100) * circumference} ${circumference}`;
  return (
    <div className="flex flex-col items-center">
      <div className="relative w-28 h-28">
        <svg className="w-full h-full transform -rotate-90">
          <circle cx="56" cy="56" r="45" stroke="#e2e8f0" strokeWidth="10" fill="none" />
          <circle cx="56" cy="56" r="45" stroke={actualColor} strokeWidth="10" fill="none" strokeDasharray={strokeDasharray} strokeLinecap="round" />
        </svg>
        <div className="absolute inset-0 flex items-center justify-center">
          <span className="text-xl font-bold" style={{ color: actualColor }}>{value}%</span>
        </div>
      </div>
      <p className="text-sm text-slate-600 mt-2 text-center">{label}</p>
    </div>
  );
};

const APAARDashboard = () => {
  const { scope } = useScope();
  const [loading, setLoading] = useState(true);
  const [importing, setImporting] = useState(false);
  const [overview, setOverview] = useState(null);
  const [funnelData, setFunnelData] = useState([]);
  const [blockData, setBlockData] = useState([]);
  const [classData, setClassData] = useState([]);
  const [topPending, setTopPending] = useState([]);
  const [lowPerforming, setLowPerforming] = useState([]);
  const [riskData, setRiskData] = useState(null);
  const [activeTab, setActiveTab] = useState("executive");

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      // Build scope parameters - the axios interceptor will merge them with localStorage values
      // Also pass district_name, block_name, school_name to help with matching
      const requestParams = {};
      if (scope.districtCode && scope.districtCode.trim()) {
        requestParams.district_code = scope.districtCode.trim();
      }
      if (scope.districtName && scope.districtName.trim()) {
        requestParams.district_name = scope.districtName.trim();
      }
      if (scope.blockCode && scope.blockCode.trim()) {
        requestParams.block_code = scope.blockCode.trim();
      }
      if (scope.blockName && scope.blockName.trim()) {
        requestParams.block_name = scope.blockName.trim();
      }
      if (scope.udiseCode && scope.udiseCode.trim()) {
        requestParams.udise_code = scope.udiseCode.trim();
      }
      if (scope.schoolName && scope.schoolName.trim()) {
        requestParams.school_name = scope.schoolName.trim();
      }

      console.log('Fetching APAAR data with scope:', {
        districtCode: scope.districtCode,
        blockCode: scope.blockCode,
        udiseCode: scope.udiseCode,
        params: requestParams,
        apiUrl: API
      });

      const [overviewRes, funnelRes, blockRes, classRes, pendingRes, lowRes, riskRes] = await Promise.all([
        axios.get(`${API}/apaar/overview`, { params: requestParams }),
        axios.get(`${API}/apaar/status-funnel`, { params: requestParams }),
        axios.get(`${API}/apaar/block-wise`, { params: requestParams }),
        axios.get(`${API}/apaar/class-wise`, { params: requestParams }),
        axios.get(`${API}/apaar/top-pending-schools`, { params: requestParams }),
        axios.get(`${API}/apaar/low-performing-schools`, { params: requestParams }),
        axios.get(`${API}/apaar/risk-schools`, { params: requestParams })
      ]);
      
      console.log('APAAR data fetched successfully:', {
        overviewTotalSchools: overviewRes?.data?.total_schools,
        overviewTotalStudents: overviewRes?.data?.total_students,
        blockDataCount: Array.isArray(blockRes?.data) ? blockRes.data.length : 0,
        hasFunnelData: Array.isArray(funnelRes?.data) && funnelRes.data.length > 0
      });
      
      setOverview(overviewRes.data);
      setFunnelData(Array.isArray(funnelRes.data) ? funnelRes.data : []);
      setBlockData(Array.isArray(blockRes.data) ? blockRes.data : []);
      setClassData(Array.isArray(classRes.data) ? classRes.data : []);
      setTopPending(Array.isArray(pendingRes.data) ? pendingRes.data : []);
      setLowPerforming(Array.isArray(lowRes.data) ? lowRes.data : []);
      setRiskData(riskRes.data || null);
    } catch (error) {
      console.error("Error fetching APAAR data:", error);
      console.error("Error details:", {
        message: error.message,
        response: error.response?.data,
        status: error.response?.status,
        url: error.config?.url,
        code: error.code
      });
      
      // Handle network errors (including ERR_BLOCKED_BY_CLIENT from browser extensions)
      if (error.code === 'ERR_NETWORK' || error.message?.includes('ERR_BLOCKED_BY_CLIENT') || error.message?.includes('Failed to fetch')) {
        console.warn("Network error detected - may be caused by browser extension");
        toast.error("Network error: Please disable ad blockers or privacy extensions and try again");
        setLoading(false);
        return;
      }
      
      const errorMsg = error.response?.data?.detail || error.message || "Failed to load APAAR data";
      
      // Only show error toast for actual errors, not for empty results
      if (error.response?.status && error.response?.status !== 404 && error.response?.status >= 500) {
        toast.error(errorMsg);
      }
      
      // Reset data on error to show "No Data" message (but not on network errors)
      if (error.response) {
        setOverview(null);
        setFunnelData([]);
        setBlockData([]);
        setClassData([]);
        setTopPending([]);
        setLowPerforming([]);
        setRiskData(null);
      }
    } finally {
      setLoading(false);
    }
  }, [scope.version, scope.districtCode, scope.blockCode, scope.udiseCode]);

  useEffect(() => { fetchData(); }, [fetchData]);

  const handleImport = async () => {
    setImporting(true);
    try {
      await axios.post(`${API}/apaar/import`);
      toast.success("APAAR import started!");
      setTimeout(() => { fetchData(); setImporting(false); }, 10000);
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
            <p key={index} className="text-sm" style={{ color: entry.color }}>{entry.name}: {typeof entry.value === 'number' ? entry.value.toLocaleString() : entry.value}</p>
          ))}
        </div>
      );
    }
    return null;
  };

  if (loading) return <div className="flex items-center justify-center h-96"><div className="loading-spinner" /></div>;

  // Check if we have data - overview should exist and have meaningful data
  const hasData = overview && typeof overview === 'object' && (
    (overview.total_schools > 0) || 
    (overview.total_students > 0) ||
    (Object.keys(overview).length > 3)
  );
  
  // Debug log to help diagnose data loading issues
  if (overview) {
    console.log('APAAR Dashboard data check:', {
      hasData,
      totalSchools: overview.total_schools,
      totalStudents: overview.total_students,
      overviewKeys: Object.keys(overview).length,
      overviewSample: Object.keys(overview).slice(0, 5)
    });
  }

  const buildInsights = () => {
    if (!overview) {
      return [
        "## Insights",
        "- No APAAR data available for the current filters.",
        "",
        "## Root Cause Signals",
        "- Missing or incomplete records in the selected scope.",
        "",
        "## Recommendations",
        "- Refresh data or broaden filters to include valid records.",
        "",
        "## Priority Action Items",
        "- Validate data import for APAAR status.",
      ].join("\n");
    }

    const fmt = (v) => (typeof v === "number" ? v.toLocaleString("en-IN") : v ?? 0);
    const pct = (v) => (typeof v === "number" ? `${v}%` : "0%");
    const low = Array.isArray(lowPerforming) ? lowPerforming.slice(0, 3) : [];

    return [
      "## Insights",
      `- APAAR generation rate: **${pct(overview.generation_rate)}** for **${fmt(overview.total_students)}** students.`,
      `- Pending: **${fmt(overview.total_pending)}** (${pct(overview.pending_pct)}), not applied: **${fmt(overview.total_not_applied)}**.`,
      `- Failed cases: **${fmt(overview.total_failed)}** (${overview.failure_rate_per_1000}/1000).`,
      low.length ? `- Lowest performing schools: **${low.map((s) => s.school_name).join(", ")}**.` : "- Lowest performing schools: unavailable.",
      "",
      "## Root Cause Signals",
      "- High pending and not-applied counts indicate onboarding/documentation bottlenecks.",
      "- Failed cases suggest data quality or verification issues.",
      "",
      "## Recommendations",
      "- Run focused verification camps for pending and failed cases.",
      "- Enforce weekly school-level tracking for not-applied students.",
      "",
      "## Priority Action Items",
      low.length ? `- Week 1: lift APAAR generation in **${low.map((s) => s.school_name).join(", ")}**.` : "- Week 1: lift APAAR generation in lowest-performing schools.",
      `- Week 2: clear **${fmt(overview.total_pending)}** pending and **${fmt(overview.total_failed)}** failed cases.`,
      `- Week 3–4: reduce **${fmt(overview.total_not_applied)}** not-applied to <5%.`,
    ].join("\n");
  };

  return (
    <div className="space-y-6 animate-fade-in" data-testid="apaar-dashboard">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight" style={{ fontFamily: 'Manrope' }}>APAAR Entry Status Dashboard</h1>
          <p className="text-slate-500 mt-1">School-wise APAAR Generation • 2025-26 • Pune District</p>
        </div>
        <div className="flex items-center gap-3">
          <Button variant="outline" size="sm" onClick={handleImport} disabled={importing} data-testid="import-apaar-btn">
            {importing ? <RefreshCw className="w-4 h-4 mr-2 animate-spin" /> : <Upload className="w-4 h-4 mr-2" />}
            {importing ? "Importing..." : "Import Data"}
          </Button>
          <ExportPanel dashboardName="apaar" dashboardTitle="APAAR Status" />
          <Button variant="outline" size="sm" onClick={fetchData} data-testid="refresh-btn"><RefreshCw className="w-4 h-4 mr-2" />Refresh</Button>
        </div>
      </div>

      {!hasData ? (
        <Card className="border-slate-200">
          <CardContent className="py-12 text-center">
            <Users className="w-16 h-16 mx-auto text-slate-300 mb-4" />
            <h3 className="text-xl font-semibold text-slate-700 mb-2">No APAAR Data Available</h3>
            <p className="text-slate-500 mb-4">Click "Import Data" to load the APAAR Entry Status Excel file</p>
            <Button onClick={handleImport} disabled={importing} data-testid="import-apaar-empty-btn">{importing ? "Importing..." : "Import APAAR Data"}</Button>
          </CardContent>
        </Card>
      ) : (
        <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
          <TabsList className="bg-slate-100">
            <TabsTrigger value="executive" data-testid="tab-executive"><BarChart3 className="w-4 h-4 mr-2" />Executive Overview</TabsTrigger>
            <TabsTrigger value="blocks" data-testid="tab-blocks"><Target className="w-4 h-4 mr-2" />Block Performance</TabsTrigger>
            <TabsTrigger value="schools" data-testid="tab-schools"><School className="w-4 h-4 mr-2" />School Action</TabsTrigger>
            <TabsTrigger value="class" data-testid="tab-class"><TrendingUp className="w-4 h-4 mr-2" />Class Analysis</TabsTrigger>
            <TabsTrigger value="insights" data-testid="tab-insights"><Brain className="w-4 h-4 mr-2" />Insights</TabsTrigger>
          </TabsList>

          {/* EXECUTIVE OVERVIEW */}
          <TabsContent value="executive" className="space-y-6">
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
              <KPICard label="Total Students" value={overview.total_students} icon={Users} color="blue" description={`${overview.total_schools} schools`} />
              <KPICard label="APAAR Generated" value={overview.total_generated} icon={CheckCircle2} color="green" />
              <KPICard label="Generation Rate" value={overview.generation_rate} suffix="%" icon={TrendingUp} color={overview.generation_rate >= 85 ? "green" : "amber"} />
              <KPICard label="Pending" value={overview.total_pending} icon={Clock} color="amber" description={`${overview.pending_pct}%`} />
              <KPICard label="Not Applied" value={overview.total_not_applied} icon={XCircle} color="red" description={`${overview.not_applied_pct}%`} />
              <KPICard label="Failed" value={overview.total_failed} icon={AlertTriangle} color="red" description={`${overview.failure_rate_per_1000}/1000`} />
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <Card className="border-slate-200">
                <CardHeader className="pb-2"><CardTitle className="text-lg">APAAR Status Funnel</CardTitle></CardHeader>
                <CardContent>
                  <div className="h-64">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={funnelData} layout="vertical" margin={{ left: 80 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis type="number" />
                        <YAxis dataKey="status" type="category" />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="count" name="Students" radius={[0, 4, 4, 0]}>
                          {funnelData.map((entry, index) => <Cell key={`cell-${index}`} fill={entry.color} />)}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>

              <Card className="border-slate-200">
                <CardHeader className="pb-2"><CardTitle className="text-lg">Key Metrics</CardTitle></CardHeader>
                <CardContent>
                  <div className="flex flex-wrap justify-around gap-4 py-4">
                    <GaugeChart value={overview.generation_rate} label="Generation Rate" />
                    <GaugeChart value={100 - overview.pending_pct} label="Completion" />
                    <GaugeChart value={100 - overview.not_applied_pct} label="Consent Coverage" />
                  </div>
                </CardContent>
              </Card>
            </div>

            {riskData && (
              <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                <Card className="border-slate-200 border-l-4 border-l-red-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500">Critical Schools (&lt;70% Gen, &gt;200 students)</p>
                    <p className="text-3xl font-bold text-red-600">{riskData.critical_schools}</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-amber-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500">High Failure Schools (&gt;50 failed)</p>
                    <p className="text-3xl font-bold text-amber-600">{riskData.high_failure_schools}</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-purple-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500">Consent Gap Schools (&gt;60% not applied)</p>
                    <p className="text-3xl font-bold text-purple-600">{riskData.consent_gap_schools}</p>
                  </CardContent>
                </Card>
              </div>
            )}
          </TabsContent>

          {/* BLOCK PERFORMANCE */}
          <TabsContent value="blocks" className="space-y-6">
            <Card className="border-slate-200">
              <CardHeader className="pb-2"><CardTitle className="text-lg">Block-wise APAAR Performance</CardTitle></CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow className="bg-slate-50">
                        <TableHead>Rank</TableHead>
                        <TableHead>Block</TableHead>
                        <TableHead className="text-right">Schools</TableHead>
                        <TableHead className="text-right">Students</TableHead>
                        <TableHead className="text-right">Generated</TableHead>
                        <TableHead className="text-right">Gen %</TableHead>
                        <TableHead className="text-right">Pending</TableHead>
                        <TableHead className="text-right">Not Applied</TableHead>
                        <TableHead className="text-right">Priority</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {blockData.map((block) => (
                        <TableRow key={block.block_name}>
                          <TableCell className="text-slate-500">{block.rank}</TableCell>
                          <TableCell className="font-medium">
                            <BlockLink blockCode={block.block_code}>{block.block_name}</BlockLink>
                          </TableCell>
                          <TableCell className="text-right">{block.total_schools}</TableCell>
                          <TableCell className="text-right tabular-nums">{block.total_students?.toLocaleString()}</TableCell>
                          <TableCell className="text-right tabular-nums text-emerald-600">{block.total_generated?.toLocaleString()}</TableCell>
                          <TableCell className="text-right"><Badge className={block.generation_rate >= 85 ? "bg-emerald-100 text-emerald-700" : block.generation_rate >= 80 ? "bg-amber-100 text-amber-700" : "bg-red-100 text-red-700"}>{block.generation_rate}%</Badge></TableCell>
                          <TableCell className="text-right tabular-nums text-amber-600">{block.pending?.toLocaleString()}</TableCell>
                          <TableCell className="text-right tabular-nums">{block.total_not_applied?.toLocaleString()}</TableCell>
                          <TableCell className="text-right"><Badge className="bg-blue-100 text-blue-700">{block.priority_index}%</Badge></TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>

            <Card className="border-slate-200">
              <CardHeader className="pb-2"><CardTitle className="text-lg">Block Generation Rate</CardTitle></CardHeader>
              <CardContent>
                <div className="h-80">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={[...blockData].sort((a, b) => b.generation_rate - a.generation_rate)} layout="vertical" margin={{ left: 80 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                      <XAxis type="number" domain={[0, 100]} />
                      <YAxis dataKey="block_name" type="category" tick={{ fontSize: 11 }} />
                      <Tooltip content={<CustomTooltip />} />
                      <Bar dataKey="generation_rate" name="Gen %" radius={[0, 4, 4, 0]}>
                        {blockData.map((entry, index) => <Cell key={`cell-${index}`} fill={entry.generation_rate >= 85 ? "#10b981" : entry.generation_rate >= 80 ? "#f59e0b" : "#ef4444"} />)}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          {/* SCHOOL ACTION */}
          <TabsContent value="schools" className="space-y-6">
            <Card className="border-slate-200 border-l-4 border-l-amber-500">
              <CardHeader className="pb-2"><CardTitle className="text-lg">Top 20 Schools by Pending Count</CardTitle></CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow className="bg-amber-50">
                        <TableHead>#</TableHead>
                        <TableHead>School Name</TableHead>
                        <TableHead>Block</TableHead>
                        <TableHead className="text-right">Students</TableHead>
                        <TableHead className="text-right">Generated</TableHead>
                        <TableHead className="text-right">Pending</TableHead>
                        <TableHead className="text-right">Gen %</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {topPending.map((school) => (
                        <TableRow key={school.udise_code}>
                          <TableCell className="text-slate-500">{school.rank}</TableCell>
                          <TableCell className="font-medium max-w-xs truncate">
                            <SchoolLink udiseCode={school.udise_code}>{school.school_name}</SchoolLink>
                          </TableCell>
                          <TableCell>
                            <BlockLink blockCode={school.block_code}>{school.block_name}</BlockLink>
                          </TableCell>
                          <TableCell className="text-right tabular-nums">{school.total_student}</TableCell>
                          <TableCell className="text-right tabular-nums text-emerald-600">{school.total_generated}</TableCell>
                          <TableCell className="text-right"><Badge className="bg-amber-100 text-amber-700">{school.pending}</Badge></TableCell>
                          <TableCell className="text-right"><Badge className={school.generation_rate >= 85 ? "bg-emerald-100 text-emerald-700" : "bg-red-100 text-red-700"}>{school.generation_rate}%</Badge></TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>

            <Card className="border-slate-200 border-l-4 border-l-red-500">
              <CardHeader className="pb-2"><CardTitle className="text-lg">Low Performing Schools (&lt;80% Generation)</CardTitle></CardHeader>
              <CardContent>
                <div className="overflow-x-auto max-h-96 overflow-y-auto">
                  <Table>
                    <TableHeader>
                      <TableRow className="bg-red-50">
                        <TableHead>School Name</TableHead>
                        <TableHead>Block</TableHead>
                        <TableHead className="text-right">Students</TableHead>
                        <TableHead className="text-right">Generated</TableHead>
                        <TableHead className="text-right">Gen %</TableHead>
                        <TableHead className="text-right">Not Applied</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {lowPerforming.slice(0, 20).map((school, idx) => (
                        <TableRow key={school.udise_code}>
                          <TableCell className="font-medium max-w-xs truncate">
                            <SchoolLink udiseCode={school.udise_code}>{school.school_name}</SchoolLink>
                          </TableCell>
                          <TableCell>
                            <BlockLink blockCode={school.block_code}>{school.block_name}</BlockLink>
                          </TableCell>
                          <TableCell className="text-right tabular-nums">{school.total_student}</TableCell>
                          <TableCell className="text-right tabular-nums">{school.total_generated}</TableCell>
                          <TableCell className="text-right"><Badge className="bg-red-100 text-red-700">{school.generation_rate}%</Badge></TableCell>
                          <TableCell className="text-right tabular-nums">{school.total_not_applied}</TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          {/* CLASS ANALYSIS */}
          <TabsContent value="class" className="space-y-6">
            <Card className="border-slate-200">
              <CardHeader className="pb-2"><CardTitle className="text-lg">Class-wise APAAR Generation Rate</CardTitle></CardHeader>
              <CardContent>
                <div className="h-80">
                  <ResponsiveContainer width="100%" height="100%">
                    <LineChart data={classData}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                      <XAxis dataKey="class" />
                      <YAxis domain={[0, 100]} />
                      <Tooltip content={<CustomTooltip />} />
                      <Line type="monotone" dataKey="generation_rate" name="Gen %" stroke="#10b981" strokeWidth={2} dot={{ fill: "#10b981", r: 4 }} />
                    </LineChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>

            <Card className="border-slate-200">
              <CardHeader className="pb-2"><CardTitle className="text-lg">Class-wise Breakdown</CardTitle></CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow className="bg-slate-50">
                        <TableHead>Class</TableHead>
                        <TableHead className="text-right">Students</TableHead>
                        <TableHead className="text-right">Generated</TableHead>
                        <TableHead className="text-right">Gen %</TableHead>
                        <TableHead className="text-right">Pending</TableHead>
                        <TableHead className="text-right">Not Applied</TableHead>
                        <TableHead className="text-right">Not Applied %</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {classData.map((cls) => (
                        <TableRow key={cls.class}>
                          <TableCell className="font-medium">{cls.class}</TableCell>
                          <TableCell className="text-right tabular-nums">{cls.total_students?.toLocaleString()}</TableCell>
                          <TableCell className="text-right tabular-nums text-emerald-600">{cls.total_generated?.toLocaleString()}</TableCell>
                          <TableCell className="text-right"><Badge className={cls.generation_rate >= 85 ? "bg-emerald-100 text-emerald-700" : "bg-amber-100 text-amber-700"}>{cls.generation_rate}%</Badge></TableCell>
                          <TableCell className="text-right tabular-nums text-amber-600">{cls.pending?.toLocaleString()}</TableCell>
                          <TableCell className="text-right tabular-nums">{cls.not_applied?.toLocaleString()}</TableCell>
                          <TableCell className="text-right"><Badge className={cls.not_applied_pct <= 10 ? "bg-emerald-100 text-emerald-700" : "bg-red-100 text-red-700"}>{cls.not_applied_pct}%</Badge></TableCell>
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
              title="APAAR Entry Status Insights"
              description="AI insights, root cause signals, recommendations, and priority actions for APAAR status."
              generate={buildInsights}
            />
          </TabsContent>
        </Tabs>
      )}
    </div>
  );
};

export default APAARDashboard;
