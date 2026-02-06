import { useState, useEffect, useCallback } from "react";
import axios from "axios";
import { useScope } from "@/context/ScopeContext";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Building2, RefreshCw, Upload, School, BarChart3, Droplets, Bath, AlertTriangle, CheckCircle2, XCircle, Heart, Users, Wrench, ShieldCheck, TrendingUp, Brain } from "lucide-react";
import { toast } from "sonner";
import ExportPanel from "@/components/ExportPanel";
import AiInsightsTab from "@/components/AiInsightsTab";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell, PieChart, Pie, Legend, LineChart, Line, RadarChart, PolarGrid, PolarAngleAxis, PolarRadiusAxis, Radar } from "recharts";

import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;

const KPICard = ({ label, value, suffix = "", icon: Icon, color = "blue", description, trend }) => {
  const colors = { 
    blue: "bg-blue-50 text-blue-600", 
    green: "bg-emerald-50 text-emerald-600", 
    red: "bg-red-50 text-red-600", 
    amber: "bg-amber-50 text-amber-600", 
    purple: "bg-purple-50 text-purple-600",
    cyan: "bg-cyan-50 text-cyan-600"
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
            {trend && <p className={`text-xs mt-1 ${trend > 0 ? 'text-emerald-500' : 'text-red-500'}`}>{trend > 0 ? '↑' : '↓'} {Math.abs(trend)}%</p>}
          </div>
          {Icon && <div className={`p-2 rounded-lg ${colors[color]}`}><Icon className="w-5 h-5" strokeWidth={1.5} /></div>}
        </div>
      </CardContent>
    </Card>
  );
};

const GaugeChart = ({ value, label, color = "#10b981" }) => {
  const getColor = (val) => val >= 90 ? "#10b981" : val >= 80 ? "#f59e0b" : "#ef4444";
  const actualColor = color || getColor(value);
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
      <p className="text-sm text-slate-600 mt-2 text-center max-w-[100px]">{label}</p>
    </div>
  );
};

const ProgressBar = ({ label, value, max, color = "bg-emerald-500" }) => {
  const pct = Math.round((value / max) * 100);
  return (
    <div className="space-y-1">
      <div className="flex justify-between text-sm">
        <span className="text-slate-600">{label}</span>
        <span className="font-medium">{pct}%</span>
      </div>
      <div className="h-2 bg-slate-100 rounded-full overflow-hidden">
        <div className={`h-full ${color} rounded-full`} style={{ width: `${pct}%` }} />
      </div>
    </div>
  );
};

const ClassroomsToiletsDashboard = () => {
  const { scope } = useScope();
  const [loading, setLoading] = useState(true);
  const [importing, setImporting] = useState(false);
  const [overview, setOverview] = useState(null);
  const [blockData, setBlockData] = useState([]);
  const [classroomCondition, setClassroomCondition] = useState(null);
  const [toiletDistribution, setToiletDistribution] = useState(null);
  const [hygieneMetrics, setHygieneMetrics] = useState(null);
  const [riskSchools, setRiskSchools] = useState(null);
  const [constructionStatus, setConstructionStatus] = useState(null);
  const [equityMetrics, setEquityMetrics] = useState(null);
  const [topBottomBlocks, setTopBottomBlocks] = useState(null);
  const [activeTab, setActiveTab] = useState("infrastructure");

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

      console.log('Fetching Classrooms & Toilets data with scope:', {
        districtCode: scope.districtCode,
        blockCode: scope.blockCode,
        udiseCode: scope.udiseCode,
        params: requestParams,
        apiUrl: API
      });

      const [overviewRes, blockRes, conditionRes, toiletRes, hygieneRes, riskRes, constructionRes, equityRes, rankRes] = await Promise.all([
        axios.get(`${API}/classrooms-toilets/overview`, { params: requestParams }),
        axios.get(`${API}/classrooms-toilets/block-wise`, { params: requestParams }),
        axios.get(`${API}/classrooms-toilets/classroom-condition`, { params: requestParams }),
        axios.get(`${API}/classrooms-toilets/toilet-distribution`, { params: requestParams }),
        axios.get(`${API}/classrooms-toilets/hygiene-metrics`, { params: requestParams }),
        axios.get(`${API}/classrooms-toilets/risk-schools`, { params: requestParams }),
        axios.get(`${API}/classrooms-toilets/construction-status`, { params: requestParams }),
        axios.get(`${API}/classrooms-toilets/equity-metrics`, { params: requestParams }),
        axios.get(`${API}/classrooms-toilets/top-bottom-blocks`, { params: requestParams })
      ]);
      
      console.log('Classrooms & Toilets data fetched successfully:', {
        overviewTotalSchools: overviewRes?.data?.total_schools,
        overviewTotalClassrooms: overviewRes?.data?.total_classrooms,
        blockDataCount: Array.isArray(blockRes?.data) ? blockRes.data.length : 0,
        hasConditionData: !!conditionRes?.data,
        hasToiletData: !!toiletRes?.data
      });
      
      setOverview(overviewRes.data);
      setBlockData(Array.isArray(blockRes.data) ? blockRes.data : []);
      setClassroomCondition(conditionRes.data || null);
      setToiletDistribution(toiletRes.data || null);
      setHygieneMetrics(hygieneRes.data || null);
      setRiskSchools(riskRes.data || null);
      setConstructionStatus(constructionRes.data || null);
      setEquityMetrics(equityRes.data || null);
      setTopBottomBlocks(rankRes.data || null);
    } catch (error) {
      console.error("Error fetching Classrooms & Toilets data:", error);
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
      
      const errorMsg = error.response?.data?.detail || error.message || "Failed to load dashboard data";
      
      // Only show error toast for actual errors, not for empty results
      if (error.response?.status && error.response?.status !== 404 && error.response?.status >= 500) {
        toast.error(errorMsg);
      }
      
      // Reset data on error to show "No Data" message (but not on network errors)
      if (error.response) {
        setOverview(null);
        setBlockData([]);
        setClassroomCondition(null);
        setToiletDistribution(null);
        setHygieneMetrics(null);
        setRiskSchools(null);
        setConstructionStatus(null);
        setEquityMetrics(null);
        setTopBottomBlocks(null);
      }
    } finally {
      setLoading(false);
    }
  }, [scope.version, scope.districtCode, scope.blockCode, scope.udiseCode]);

  useEffect(() => { fetchData(); }, [fetchData]);

  const handleImport = async () => {
    setImporting(true);
    try {
      await axios.post(`${API}/classrooms-toilets/import`);
      toast.success("Import started! Data will be ready shortly.");
      setTimeout(() => { fetchData(); setImporting(false); }, 12000);
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
    (overview.total_classrooms > 0) ||
    (Object.keys(overview).length > 3)
  );
  
  // Debug log to help diagnose data loading issues
  if (overview) {
    console.log('Classrooms & Toilets Dashboard data check:', {
      hasData,
      totalSchools: overview.total_schools,
      totalClassrooms: overview.total_classrooms,
      overviewKeys: Object.keys(overview).length,
      overviewSample: Object.keys(overview).slice(0, 5)
    });
  }

  const buildInsights = () => {
    if (!overview) {
      return [
        "## Insights",
        "- No classrooms & toilets data available for the current filters.",
        "",
        "## Root Cause Signals",
        "- Missing or incomplete records in the selected scope.",
        "",
        "## Recommendations",
        "- Refresh data or broaden filters to include valid records.",
        "",
        "## Priority Action Items",
        "- Validate data import for classrooms & toilets.",
      ].join("\n");
    }

    const fmt = (v) => (typeof v === "number" ? v.toLocaleString("en-IN") : v ?? 0);
    const pct = (v) => (typeof v === "number" ? `${v}%` : "0%");

    return [
      "## Insights",
      `- Total classrooms: **${fmt(overview.total_classrooms)}** with classroom health index **${pct(overview.classroom_health_index)}**.`,
      `- Repair backlog: **${pct(overview.repair_backlog_pct)}**; infrastructure readiness **${pct(overview.infrastructure_readiness_index)}**.`,
      `- Toilet functional rate: **${pct(overview.toilet_functional_pct)}**, water coverage **${pct(overview.water_coverage_pct)}**.`,
      "",
      "## Root Cause Signals",
      "- High repair backlog indicates deferred maintenance and funding gaps.",
      "- Low water coverage and functional toilet rates suggest O&M issues.",
      "",
      "## Recommendations",
      "- Prioritize major repairs in high-risk schools and blocks.",
      "- Ensure water supply and O&M contracts for toilet functionality.",
      "",
      "## Priority Action Items",
      `- Week 1: fix major repairs (**${fmt(overview.classrooms_major_repair)}**) and zero-toilet schools.`,
      `- Week 2: raise toilet functional rate (**${pct(overview.toilet_functional_pct)}**) and water coverage (**${pct(overview.water_coverage_pct)}**).`,
      `- Week 3–4: reduce repair backlog (**${pct(overview.repair_backlog_pct)}**) and re-score health index.`,
    ].join("\n");
  };

  return (
    <div className="space-y-6 animate-fade-in" data-testid="classrooms-toilets-dashboard">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight" style={{ fontFamily: 'Manrope' }}>Classrooms & Toilets Dashboard</h1>
          <p className="text-slate-500 mt-1">Infrastructure & WASH Compliance • AY 2025-26 • Pune District</p>
        </div>
        <div className="flex items-center gap-3">
          <Button variant="outline" size="sm" onClick={handleImport} disabled={importing} data-testid="import-btn">
            {importing ? <RefreshCw className="w-4 h-4 mr-2 animate-spin" /> : <Upload className="w-4 h-4 mr-2" />}
            {importing ? "Importing..." : "Import Data"}
          </Button>
          <ExportPanel dashboardName="classrooms-toilets" dashboardTitle="Classrooms & Toilets" />
          <Button variant="outline" size="sm" onClick={fetchData} data-testid="refresh-btn"><RefreshCw className="w-4 h-4 mr-2" />Refresh</Button>
        </div>
      </div>

      {!hasData ? (
        <Card className="border-slate-200">
          <CardContent className="py-12 text-center">
            <Building2 className="w-16 h-16 mx-auto text-slate-300 mb-4" />
            <h3 className="text-xl font-semibold text-slate-700 mb-2">No Data Available</h3>
            <p className="text-slate-500 mb-4">Click "Import Data" to load Classrooms & Toilets data</p>
            <Button onClick={handleImport} disabled={importing} data-testid="import-empty-btn">{importing ? "Importing..." : "Import Data"}</Button>
          </CardContent>
        </Card>
      ) : (
        <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
          <TabsList className="bg-slate-100 flex-wrap h-auto gap-1 p-1">
            <TabsTrigger value="infrastructure" data-testid="tab-infrastructure"><Building2 className="w-4 h-4 mr-2" />Infrastructure</TabsTrigger>
            <TabsTrigger value="toilets" data-testid="tab-toilets"><Bath className="w-4 h-4 mr-2" />Toilets & Water</TabsTrigger>
            <TabsTrigger value="hygiene" data-testid="tab-hygiene"><Droplets className="w-4 h-4 mr-2" />Hygiene & WASH</TabsTrigger>
            <TabsTrigger value="equity" data-testid="tab-equity"><Users className="w-4 h-4 mr-2" />Equity & Inclusion</TabsTrigger>
            <TabsTrigger value="risk" data-testid="tab-risk"><AlertTriangle className="w-4 h-4 mr-2" />Risk & Action</TabsTrigger>
            <TabsTrigger value="insights" data-testid="tab-insights"><Brain className="w-4 h-4 mr-2" />Insights</TabsTrigger>
          </TabsList>

          {/* INFRASTRUCTURE TAB */}
          <TabsContent value="infrastructure" className="space-y-6">
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
              <KPICard label="Total Schools" value={overview.total_schools} icon={School} color="blue" description={`${overview.total_blocks} blocks`} />
              <KPICard label="Total Classrooms" value={overview.total_classrooms} icon={Building2} color="purple" description={`${overview.avg_classrooms_per_school} avg/school`} />
              <KPICard label="Good Condition" value={overview.classrooms_good} icon={CheckCircle2} color="green" />
              <KPICard label="Minor Repair" value={overview.classrooms_minor_repair} icon={Wrench} color="amber" />
              <KPICard label="Major Repair" value={overview.classrooms_major_repair} icon={AlertTriangle} color="red" />
              <KPICard label="Under Construction" value={overview.classrooms_under_construction} icon={Building2} color="cyan" />
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <Card className="border-slate-200">
                <CardHeader className="pb-2"><CardTitle className="text-lg">Classroom Health Metrics</CardTitle></CardHeader>
                <CardContent>
                  <div className="flex flex-wrap justify-around gap-4 py-4">
                    <GaugeChart value={overview.classroom_health_index} label="Classroom Health Index" />
                    <GaugeChart value={100 - overview.repair_backlog_pct} label="Asset Readiness" />
                    <GaugeChart value={overview.infrastructure_readiness_index} label="Infrastructure Index" />
                  </div>
                </CardContent>
              </Card>

              <Card className="border-slate-200">
                <CardHeader className="pb-2"><CardTitle className="text-lg">Classroom Condition Distribution</CardTitle></CardHeader>
                <CardContent>
                  {classroomCondition && (
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie data={classroomCondition.by_condition} dataKey="count" nameKey="condition" cx="50%" cy="50%" outerRadius={80} label={({ name, percent }) => `${name} ${(percent * 100).toFixed(0)}%`}>
                            {classroomCondition.by_condition.map((entry, index) => <Cell key={`cell-${index}`} fill={entry.color} />)}
                          </Pie>
                          <Tooltip content={<CustomTooltip />} />
                          <Legend />
                        </PieChart>
                      </ResponsiveContainer>
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>

            <Card className="border-slate-200">
              <CardHeader className="pb-2"><CardTitle className="text-lg">Block-wise Infrastructure Performance</CardTitle></CardHeader>
              <CardContent>
                <div className="overflow-x-auto">
                  <Table>
                    <TableHeader>
                      <TableRow className="bg-slate-50">
                        <TableHead>Rank</TableHead>
                        <TableHead>Block</TableHead>
                        <TableHead className="text-right">Schools</TableHead>
                        <TableHead className="text-right">Classrooms</TableHead>
                        <TableHead className="text-right">Avg/School</TableHead>
                        <TableHead className="text-right">Health Index</TableHead>
                        <TableHead className="text-right">Repair Backlog</TableHead>
                        <TableHead className="text-right">WASH Index</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {blockData.slice(0, 10).map((block) => (
                        <TableRow key={block.block_name}>
                          <TableCell className="text-slate-500">{block.rank}</TableCell>
                          <TableCell className="font-medium">{block.block_name}</TableCell>
                          <TableCell className="text-right tabular-nums">{block.total_schools}</TableCell>
                          <TableCell className="text-right tabular-nums">{block.total_classrooms?.toLocaleString()}</TableCell>
                          <TableCell className="text-right">{block.avg_classrooms}</TableCell>
                          <TableCell className="text-right"><Badge className={block.classroom_health_index >= 90 ? "bg-emerald-100 text-emerald-700" : block.classroom_health_index >= 80 ? "bg-amber-100 text-amber-700" : "bg-red-100 text-red-700"}>{block.classroom_health_index}%</Badge></TableCell>
                          <TableCell className="text-right"><Badge className={block.repair_backlog_pct <= 5 ? "bg-emerald-100 text-emerald-700" : "bg-amber-100 text-amber-700"}>{block.repair_backlog_pct}%</Badge></TableCell>
                          <TableCell className="text-right"><Badge className="bg-blue-100 text-blue-700">{block.wash_index}%</Badge></TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          {/* TOILETS & WATER TAB */}
          <TabsContent value="toilets" className="space-y-6">
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
              <KPICard label="Total Toilets" value={overview.total_toilets} icon={Bath} color="blue" />
              <KPICard label="Functional" value={overview.total_functional} suffix="%" icon={CheckCircle2} color="green" description={`${overview.toilet_functional_pct}%`} />
              <KPICard label="With Water" value={overview.water_coverage_pct} suffix="%" icon={Droplets} color="cyan" />
              <KPICard label="Water Gap" value={overview.water_gap} icon={AlertTriangle} color="amber" description="Functional but no water" />
              <KPICard label="Boys Toilets" value={overview.boys_toilets_total} icon={Users} color="blue" description={`${overview.boys_functional_pct}% functional`} />
              <KPICard label="Girls Toilets" value={overview.girls_toilets_total} icon={Users} color="purple" description={`${overview.girls_functional_pct}% functional`} />
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <Card className="border-slate-200">
                <CardHeader className="pb-2"><CardTitle className="text-lg">Toilet Compliance Metrics</CardTitle></CardHeader>
                <CardContent>
                  <div className="flex flex-wrap justify-around gap-4 py-4">
                    <GaugeChart value={overview.toilet_functional_pct} label="Functional Rate" />
                    <GaugeChart value={overview.water_coverage_pct} label="Water Coverage" color="#3b82f6" />
                    <GaugeChart value={overview.wash_compliance_index} label="WASH Compliance" color="#8b5cf6" />
                  </div>
                </CardContent>
              </Card>

              <Card className="border-slate-200">
                <CardHeader className="pb-2"><CardTitle className="text-lg">Toilet Status Distribution</CardTitle></CardHeader>
                <CardContent>
                  {toiletDistribution && (
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={toiletDistribution.by_gender} layout="vertical" margin={{ left: 60 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis type="number" />
                          <YAxis dataKey="gender" type="category" />
                          <Tooltip content={<CustomTooltip />} />
                          <Legend />
                          <Bar dataKey="total" name="Total" fill="#94a3b8" />
                          <Bar dataKey="functional" name="Functional" fill="#10b981" />
                          <Bar dataKey="with_water" name="With Water" fill="#3b82f6" />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>

            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <Card className="border-slate-200 border-l-4 border-l-red-500">
                <CardHeader className="pb-2"><CardTitle className="text-lg">Zero Toilet Schools</CardTitle></CardHeader>
                <CardContent>
                  <div className="grid grid-cols-2 gap-4">
                    <div className="text-center p-4 bg-red-50 rounded-lg">
                      <p className="text-3xl font-bold text-red-600">{overview.zero_boys_toilet_schools}</p>
                      <p className="text-sm text-slate-500">No Boys Toilets</p>
                      <p className="text-xs text-red-500">{overview.zero_boys_pct}% of schools</p>
                    </div>
                    <div className="text-center p-4 bg-red-50 rounded-lg">
                      <p className="text-3xl font-bold text-red-600">{overview.zero_girls_toilet_schools}</p>
                      <p className="text-sm text-slate-500">No Girls Toilets</p>
                      <p className="text-xs text-red-500">{overview.zero_girls_pct}% of schools</p>
                    </div>
                  </div>
                </CardContent>
              </Card>

              <Card className="border-slate-200 border-l-4 border-l-cyan-500">
                <CardHeader className="pb-2"><CardTitle className="text-lg">Construction Pipeline</CardTitle></CardHeader>
                <CardContent>
                  {constructionStatus && (
                    <div className="space-y-4">
                      <div className="flex justify-between items-center">
                        <span className="text-slate-600">Toilets Under Construction</span>
                        <Badge className="bg-cyan-100 text-cyan-700">{constructionStatus.toilets_under_construction}</Badge>
                      </div>
                      <div className="flex justify-between items-center">
                        <span className="text-slate-600">Classrooms Under Construction</span>
                        <Badge className="bg-cyan-100 text-cyan-700">{constructionStatus.classrooms_under_construction}</Badge>
                      </div>
                      <div className="flex justify-between items-center">
                        <span className="text-slate-600">Dilapidated Classrooms</span>
                        <Badge className="bg-red-100 text-red-700">{constructionStatus.classrooms_dilapidated}</Badge>
                      </div>
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>
          </TabsContent>

          {/* HYGIENE & WASH TAB */}
          <TabsContent value="hygiene" className="space-y-6">
            <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
              <KPICard label="Handwash Coverage" value={overview.handwash_coverage_pct} suffix="%" icon={Droplets} color="cyan" />
              <KPICard label="Handwash Points" value={overview.handwash_points} icon={Droplets} color="blue" />
              <KPICard label="Sufficiency Ratio" value={overview.handwash_sufficiency_ratio} icon={CheckCircle2} color="green" description="Points per toilet" />
              <KPICard label="Sanitary Pad" value={overview.sanitary_pad_pct} suffix="%" icon={Heart} color="purple" />
              <KPICard label="Electricity" value={overview.electricity_pct} suffix="%" icon={ShieldCheck} color="amber" />
            </div>

            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              <Card className="border-slate-200">
                <CardHeader className="pb-2"><CardTitle className="text-lg">WASH Compliance Radar</CardTitle></CardHeader>
                <CardContent>
                  {hygieneMetrics && (
                    <div className="h-80">
                      <ResponsiveContainer width="100%" height="100%">
                        <RadarChart data={[
                          { metric: "Handwash", value: hygieneMetrics.handwash_coverage },
                          { metric: "Sanitary Pad", value: hygieneMetrics.sanitary_pad_coverage },
                          { metric: "Electricity", value: hygieneMetrics.electricity_coverage },
                          { metric: "Toilet Functional", value: overview.toilet_functional_pct },
                          { metric: "Water Coverage", value: overview.water_coverage_pct }
                        ]}>
                          <PolarGrid />
                          <PolarAngleAxis dataKey="metric" tick={{ fontSize: 11 }} />
                          <PolarRadiusAxis angle={30} domain={[0, 100]} />
                          <Radar name="Coverage %" dataKey="value" stroke="#8b5cf6" fill="#8b5cf6" fillOpacity={0.5} />
                          <Tooltip />
                        </RadarChart>
                      </ResponsiveContainer>
                    </div>
                  )}
                </CardContent>
              </Card>

              <Card className="border-slate-200">
                <CardHeader className="pb-2"><CardTitle className="text-lg">Hygiene Facility Distribution</CardTitle></CardHeader>
                <CardContent>
                  {hygieneMetrics && hygieneMetrics.distribution && (
                    <div className="h-80">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={hygieneMetrics.distribution} layout="vertical" margin={{ left: 100 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis type="number" />
                          <YAxis dataKey="facility" type="category" tick={{ fontSize: 11 }} />
                          <Tooltip content={<CustomTooltip />} />
                          <Legend />
                          <Bar dataKey="yes" name="Available" fill="#10b981" stackId="a" />
                          <Bar dataKey="no" name="Not Available" fill="#ef4444" stackId="a" />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  )}
                </CardContent>
              </Card>
            </div>

            <Card className="border-slate-200">
              <CardHeader className="pb-2"><CardTitle className="text-lg">Block-wise Hygiene Performance</CardTitle></CardHeader>
              <CardContent>
                <div className="h-80">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={[...blockData].sort((a, b) => b.handwash_pct - a.handwash_pct).slice(0, 15)} layout="vertical" margin={{ left: 100 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                      <XAxis type="number" domain={[0, 100]} />
                      <YAxis dataKey="block_name" type="category" tick={{ fontSize: 11 }} />
                      <Tooltip content={<CustomTooltip />} />
                      <Legend />
                      <Bar dataKey="handwash_pct" name="Handwash %" fill="#06b6d4" />
                      <Bar dataKey="sanitary_pct" name="Sanitary Pad %" fill="#8b5cf6" />
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>
          </TabsContent>

          {/* EQUITY & INCLUSION TAB */}
          <TabsContent value="equity" className="space-y-6">
            {equityMetrics && (
              <>
                <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
                  <KPICard label="Gender Parity Index" value={equityMetrics.gender_parity_index} icon={Users} color="purple" description="Girls:Boys ratio" />
                  <KPICard label="Girls Toilets" value={equityMetrics.girls_toilets} icon={Users} color="purple" />
                  <KPICard label="Boys Toilets" value={equityMetrics.boys_toilets} icon={Users} color="blue" />
                  <KPICard label="CWSN Coverage" value={equityMetrics.cwsn_coverage_pct} suffix="%" icon={Heart} color="green" />
                  <KPICard label="Menstrual Hygiene" value={equityMetrics.menstrual_hygiene_coverage} suffix="%" icon={Heart} color="purple" />
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                  <Card className="border-slate-200">
                    <CardHeader className="pb-2"><CardTitle className="text-lg">CWSN Accessibility</CardTitle></CardHeader>
                    <CardContent>
                      <div className="space-y-4 py-4">
                        <div className="grid grid-cols-2 gap-4">
                          <div className="text-center p-4 bg-emerald-50 rounded-lg">
                            <p className="text-3xl font-bold text-emerald-600">{equityMetrics.cwsn_total}</p>
                            <p className="text-sm text-slate-500">CWSN Toilets Total</p>
                          </div>
                          <div className="text-center p-4 bg-emerald-50 rounded-lg">
                            <p className="text-3xl font-bold text-emerald-600">{equityMetrics.cwsn_functional_pct}%</p>
                            <p className="text-sm text-slate-500">Functional</p>
                          </div>
                        </div>
                        <ProgressBar label="CWSN Coverage" value={equityMetrics.cwsn_functional} max={equityMetrics.cwsn_total} />
                      </div>
                    </CardContent>
                  </Card>

                  <Card className="border-slate-200">
                    <CardHeader className="pb-2"><CardTitle className="text-lg">Gender Infrastructure Gap</CardTitle></CardHeader>
                    <CardContent>
                      <div className="space-y-4 py-4">
                        <div className="flex items-center justify-between">
                          <div className="text-center">
                            <p className="text-2xl font-bold text-blue-600">{equityMetrics.boys_toilets?.toLocaleString()}</p>
                            <p className="text-sm text-slate-500">Boys Toilets</p>
                          </div>
                          <div className="text-center px-4">
                            <p className={`text-lg font-bold ${equityMetrics.toilet_gap >= 0 ? 'text-emerald-600' : 'text-red-600'}`}>
                              {equityMetrics.toilet_gap >= 0 ? '+' : ''}{equityMetrics.toilet_gap?.toLocaleString()}
                            </p>
                            <p className="text-xs text-slate-400">Gap</p>
                          </div>
                          <div className="text-center">
                            <p className="text-2xl font-bold text-purple-600">{equityMetrics.girls_toilets?.toLocaleString()}</p>
                            <p className="text-sm text-slate-500">Girls Toilets</p>
                          </div>
                        </div>
                        <div className="text-center p-3 bg-amber-50 rounded-lg">
                          <p className="text-sm text-amber-700"><strong>{equityMetrics.schools_zero_girls_toilet}</strong> schools without girls toilets</p>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                </div>
              </>
            )}
          </TabsContent>

          {/* RISK & ACTION TAB */}
          <TabsContent value="risk" className="space-y-6">
            {riskSchools && (
              <>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <Card className="border-l-4 border-l-red-500">
                    <CardContent className="p-4">
                      <p className="text-xs text-slate-500">Zero Toilet Schools</p>
                      <p className="text-3xl font-bold text-red-600">{riskSchools.risk_summary.zero_toilet_schools}</p>
                      <p className="text-xs text-red-500">{riskSchools.risk_summary.zero_toilet_pct}% of total</p>
                    </CardContent>
                  </Card>
                  <Card className="border-l-4 border-l-amber-500">
                    <CardContent className="p-4">
                      <p className="text-xs text-slate-500">No Water Schools</p>
                      <p className="text-3xl font-bold text-amber-600">{riskSchools.risk_summary.no_water_schools}</p>
                    </CardContent>
                  </Card>
                  <Card className="border-l-4 border-l-orange-500">
                    <CardContent className="p-4">
                      <p className="text-xs text-slate-500">No Handwash</p>
                      <p className="text-3xl font-bold text-orange-600">{riskSchools.risk_summary.no_handwash_schools}</p>
                      <p className="text-xs text-orange-500">{riskSchools.risk_summary.no_handwash_pct}% of total</p>
                    </CardContent>
                  </Card>
                  <Card className="border-l-4 border-l-purple-500">
                    <CardContent className="p-4">
                      <p className="text-xs text-slate-500">Major Repair Needed</p>
                      <p className="text-3xl font-bold text-purple-600">{riskSchools.risk_summary.major_repair_schools}</p>
                    </CardContent>
                  </Card>
                </div>

                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                  {topBottomBlocks && (
                    <>
                      <Card className="border-slate-200 border-l-4 border-l-emerald-500">
                        <CardHeader className="pb-2"><CardTitle className="text-lg">Top Performing Blocks</CardTitle></CardHeader>
                        <CardContent>
                          <div className="overflow-x-auto">
                            <Table>
                              <TableHeader>
                                <TableRow className="bg-emerald-50">
                                  <TableHead>Block</TableHead>
                                  <TableHead className="text-right">Health</TableHead>
                                  <TableHead className="text-right">Toilet %</TableHead>
                                  <TableHead className="text-right">Score</TableHead>
                                </TableRow>
                              </TableHeader>
                              <TableBody>
                                {topBottomBlocks.top_blocks.map((block, idx) => (
                                  <TableRow key={idx}>
                                    <TableCell className="font-medium">{block.block_name}</TableCell>
                                    <TableCell className="text-right">{block.classroom_health}%</TableCell>
                                    <TableCell className="text-right">{block.toilet_functional_pct}%</TableCell>
                                    <TableCell className="text-right"><Badge className="bg-emerald-100 text-emerald-700">{block.composite_score}</Badge></TableCell>
                                  </TableRow>
                                ))}
                              </TableBody>
                            </Table>
                          </div>
                        </CardContent>
                      </Card>

                      <Card className="border-slate-200 border-l-4 border-l-red-500">
                        <CardHeader className="pb-2"><CardTitle className="text-lg">Bottom Performing Blocks</CardTitle></CardHeader>
                        <CardContent>
                          <div className="overflow-x-auto">
                            <Table>
                              <TableHeader>
                                <TableRow className="bg-red-50">
                                  <TableHead>Block</TableHead>
                                  <TableHead className="text-right">Health</TableHead>
                                  <TableHead className="text-right">Toilet %</TableHead>
                                  <TableHead className="text-right">Score</TableHead>
                                </TableRow>
                              </TableHeader>
                              <TableBody>
                                {topBottomBlocks.bottom_blocks.map((block, idx) => (
                                  <TableRow key={idx}>
                                    <TableCell className="font-medium">{block.block_name}</TableCell>
                                    <TableCell className="text-right">{block.classroom_health}%</TableCell>
                                    <TableCell className="text-right">{block.toilet_functional_pct}%</TableCell>
                                    <TableCell className="text-right"><Badge className="bg-red-100 text-red-700">{block.composite_score}</Badge></TableCell>
                                  </TableRow>
                                ))}
                              </TableBody>
                            </Table>
                          </div>
                        </CardContent>
                      </Card>
                    </>
                  )}
                </div>

                <Card className="border-slate-200">
                  <CardHeader className="pb-2"><CardTitle className="text-lg">High-Risk Schools - Action Required</CardTitle></CardHeader>
                  <CardContent>
                    <div className="overflow-x-auto max-h-96 overflow-y-auto">
                      <Table>
                        <TableHeader>
                          <TableRow className="bg-red-50">
                            <TableHead>UDISE</TableHead>
                            <TableHead>School Name</TableHead>
                            <TableHead>Block</TableHead>
                            <TableHead className="text-center">Boys Toilet</TableHead>
                            <TableHead className="text-center">Girls Toilet</TableHead>
                            <TableHead className="text-center">Risk Flag</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {riskSchools.zero_toilet_schools.slice(0, 20).map((school, idx) => (
                            <TableRow key={idx}>
                              <TableCell className="font-mono text-xs">{school.udise_code}</TableCell>
                              <TableCell className="font-medium max-w-xs truncate">{school.school_name}</TableCell>
                              <TableCell>{school.block_name}</TableCell>
                              <TableCell className="text-center">{school.boys_toilets_total === 0 ? <XCircle className="w-4 h-4 text-red-500 mx-auto" /> : school.boys_toilets_total}</TableCell>
                              <TableCell className="text-center">{school.girls_toilets_total === 0 ? <XCircle className="w-4 h-4 text-red-500 mx-auto" /> : school.girls_toilets_total}</TableCell>
                              <TableCell className="text-center"><Badge className="bg-red-100 text-red-700">Critical</Badge></TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  </CardContent>
                </Card>
              </>
            )}
          </TabsContent>
          <TabsContent value="insights" className="space-y-6">
            <AiInsightsTab
              title="Classrooms & Toilets Insights"
              description="AI insights, root cause signals, recommendations, and priority actions for classrooms and toilets."
              generate={buildInsights}
            />
          </TabsContent>
        </Tabs>
      )}
    </div>
  );
};

export default ClassroomsToiletsDashboard;
