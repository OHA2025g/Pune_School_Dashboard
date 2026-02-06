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
  RefreshCw,
  Upload,
  School,
  TrendingUp,
  TrendingDown,
  BarChart3,
  Target,
  Scale,
  Baby,
  GraduationCap,
  Building2,
  PieChart as PieChartIcon,
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
  Area,
  AreaChart,
  ComposedChart
} from "recharts";

import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;

// KPI Card Component
const KPICard = ({ label, value, suffix = "", icon: Icon, color = "blue", description, subValue }) => {
  const colors = {
    blue: "bg-blue-50 text-blue-600",
    green: "bg-emerald-50 text-emerald-600",
    red: "bg-red-50 text-red-600",
    amber: "bg-amber-50 text-amber-600",
    purple: "bg-purple-50 text-purple-600",
    cyan: "bg-cyan-50 text-cyan-600",
    pink: "bg-pink-50 text-pink-600",
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
            {subValue && <p className="text-sm font-medium text-slate-600 mt-1">{subValue}</p>}
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

// GPI Indicator
const GPIIndicator = ({ value, size = "md" }) => {
  const getColor = (gpi) => {
    if (gpi >= 0.97 && gpi <= 1.03) return { bg: "bg-emerald-500", text: "text-emerald-700", label: "Parity" };
    if (gpi >= 0.90) return { bg: "bg-amber-500", text: "text-amber-700", label: "Near Parity" };
    return { bg: "bg-red-500", text: "text-red-700", label: "Gap" };
  };
  
  const { text, label } = getColor(value);
  const sizeClass = size === "lg" ? "text-3xl" : "text-xl";
  
  return (
    <div className="text-center">
      <p className={`font-bold ${sizeClass} ${text}`}>{value}</p>
      <p className="text-xs text-slate-500">{label}</p>
    </div>
  );
};

const AgeEnrolmentDashboard = () => {
  const { scope } = useScope();
  const [loading, setLoading] = useState(true);
  const [importing, setImporting] = useState(false);
  const [overview, setOverview] = useState(null);
  const [ageData, setAgeData] = useState([]);
  const [blockData, setBlockData] = useState([]);
  const [managementData, setManagementData] = useState([]);
  const [categoryData, setCategoryData] = useState([]);
  const [topSchools, setTopSchools] = useState([]);
  const [schoolSizeData, setSchoolSizeData] = useState(null);
  const [genderByAge, setGenderByAge] = useState([]);
  const [dataQuality, setDataQuality] = useState(null);
  const [activeTab, setActiveTab] = useState("executive");

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [
        overviewRes,
        ageRes,
        blockRes,
        mgmtRes,
        catRes,
        topRes,
        sizeRes,
        genderRes,
        qualityRes
      ] = await Promise.all([
        axios.get(`${API}/age-enrolment/overview`),
        axios.get(`${API}/age-enrolment/age-wise`),
        axios.get(`${API}/age-enrolment/block-wise`),
        axios.get(`${API}/age-enrolment/management-wise`),
        axios.get(`${API}/age-enrolment/category-wise`),
        axios.get(`${API}/age-enrolment/top-schools`),
        axios.get(`${API}/age-enrolment/school-size-distribution`),
        axios.get(`${API}/age-enrolment/gender-by-age`),
        axios.get(`${API}/age-enrolment/data-quality`)
      ]);
      
      setOverview(overviewRes.data);
      setAgeData(ageRes.data);
      setBlockData(blockRes.data);
      setManagementData(mgmtRes.data);
      setCategoryData(catRes.data);
      setTopSchools(topRes.data);
      setSchoolSizeData(sizeRes.data);
      setGenderByAge(genderRes.data);
      setDataQuality(qualityRes.data);
    } catch (error) {
      console.error("Error fetching Age Enrolment data:", error);
      toast.error("Failed to load age-wise enrolment data");
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
      await axios.post(`${API}/age-enrolment/import`);
      toast.success("Age-wise Enrolment import started!");
      setTimeout(() => {
        fetchData();
        setImporting(false);
      }, 15000); // Larger file needs more time
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
        "- No age-wise enrolment data available for the current filters.",
        "",
        "## Root Cause Signals",
        "- Missing or incomplete records in the selected scope.",
        "",
        "## Recommendations",
        "- Refresh data or broaden filters to include valid records.",
        "",
        "## Priority Action Items",
        "- Validate data import for age-wise enrolment.",
      ].join("\n");
    }

    const fmt = (v) => (typeof v === "number" ? v.toLocaleString("en-IN") : v ?? 0);
    const blocks = Array.isArray(blockData) ? blockData : [];
    const lowGpi = [...blocks].filter((b) => b.gpi !== undefined).sort((a, b) => (a.gpi || 0) - (b.gpi || 0)).slice(0, 3);

    return [
      "## Insights",
      `- Total enrolment: **${fmt(overview.total_enrolment)}** across **${fmt(overview.total_schools)}** schools.`,
      `- Gender Parity Index: **${overview.gender_parity_index}**, girls participation **${overview.girls_pct}%**.`,
      `- Secondary retention index: **${overview.secondary_retention_index}**.`,
      lowGpi.length ? `- Lowest GPI blocks: **${lowGpi.map((b) => b.block_name).join(", ")}**.` : "- Lowest GPI blocks: unavailable.",
      "",
      "## Root Cause Signals",
      "- Low GPI indicates gender participation gaps in specific blocks.",
      "- Secondary retention drop suggests transition barriers at ages 14–18.",
      "",
      "## Recommendations",
      "- Target gender-focused enrolment drives in low GPI blocks.",
      "- Strengthen transition support and retention tracking for secondary age groups.",
      "",
      "## Priority Action Items",
      lowGpi.length ? `- Week 1: close GPI gaps in **${lowGpi.map((b) => b.block_name).join(", ")}**.` : "- Week 1: close GPI gaps in lowest GPI blocks.",
      `- Week 2: lift secondary retention index (**${overview.secondary_retention_index}**) via transition support.`,
      "- Week 3–4: review parity gaps and update block plans.",
    ].join("\n");
  };

  // Prepare age trend data (filter main ages 4-18)
  const ageTrendData = ageData.filter(a => {
    const ageNum = parseInt(a.age);
    return !isNaN(ageNum) && ageNum >= 4 && ageNum <= 18;
  });

  // Management pie data
  const managementPieData = managementData.slice(0, 6).map((m, idx) => ({
    name: m.management_name.length > 20 ? m.management_name.substring(0, 20) + "..." : m.management_name,
    value: m.total_enrolment,
    color: ["#3b82f6", "#10b981", "#f59e0b", "#ef4444", "#8b5cf6", "#06b6d4"][idx]
  }));

  return (
    <div className="space-y-6 animate-fade-in" data-testid="age-enrolment-dashboard">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight" style={{ fontFamily: 'Manrope' }}>
            Age-wise Enrolment Dashboard
          </h1>
          <p className="text-slate-500 mt-1">Student Enrolment by Age • 2025-26 • Pune District</p>
        </div>
        <div className="flex items-center gap-3">
          <Button 
            variant="outline" 
            size="sm" 
            onClick={handleImport}
            disabled={importing}
            data-testid="import-age-enrolment-btn"
          >
            {importing ? <RefreshCw className="w-4 h-4 mr-2 animate-spin" /> : <Upload className="w-4 h-4 mr-2" />}
            {importing ? "Importing..." : "Import Data"}
          </Button>
          <ExportPanel dashboardName="age-enrolment" dashboardTitle="Age-wise Enrolment" />
          <Button variant="outline" size="sm" onClick={fetchData} data-testid="refresh-btn">
            <RefreshCw className="w-4 h-4 mr-2" />
            Refresh
          </Button>
        </div>
      </div>

      {!hasData ? (
        <Card className="border-slate-200">
          <CardContent className="py-12 text-center">
            <Users className="w-16 h-16 mx-auto text-slate-300 mb-4" />
            <h3 className="text-xl font-semibold text-slate-700 mb-2">No Age-wise Enrolment Data Available</h3>
            <p className="text-slate-500 mb-4">Click "Import Data" to load the Age-wise Enrolment Excel file</p>
            <Button onClick={handleImport} disabled={importing} data-testid="import-age-enrolment-empty-btn">
              {importing ? "Importing..." : "Import Age-wise Data"}
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
              <TabsTrigger value="age-gender" className="flex items-center gap-2" data-testid="tab-age-gender">
                <Users className="w-4 h-4" />
                Age & Gender
              </TabsTrigger>
              <TabsTrigger value="geography" className="flex items-center gap-2" data-testid="tab-geography">
                <Target className="w-4 h-4" />
                Geography & Schools
              </TabsTrigger>
              <TabsTrigger value="policy" className="flex items-center gap-2" data-testid="tab-policy">
                <Building2 className="w-4 h-4" />
                Policy & Management
              </TabsTrigger>
              <TabsTrigger value="insights" className="flex items-center gap-2" data-testid="tab-insights">
                <Brain className="w-4 h-4" />
                Insights
              </TabsTrigger>
            </TabsList>

            {/* DASHBOARD 1: EXECUTIVE OVERVIEW */}
            <TabsContent value="executive" className="space-y-6">
              {/* Top KPI Strip */}
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
                <KPICard
                  label="Total Enrolment"
                  value={overview.total_enrolment}
                  icon={Users}
                  color="blue"
                  description={`${overview.total_schools} schools`}
                />
                <KPICard
                  label="Gender Parity Index"
                  value={overview.gender_parity_index}
                  icon={Scale}
                  color={overview.gender_parity_index >= 0.95 ? "green" : "amber"}
                  description={overview.gender_parity_index >= 0.97 ? "Near Parity" : "Girls Lag"}
                />
                <KPICard
                  label="Girls Participation"
                  value={overview.girls_pct}
                  suffix="%"
                  icon={Users}
                  color="pink"
                  description={`${overview.total_girls?.toLocaleString()} girls`}
                />
                <KPICard
                  label="Peak Enrolment Age"
                  value={overview.peak_enrolment_age}
                  suffix="yrs"
                  icon={TrendingUp}
                  color="purple"
                  description={`${overview.peak_enrolment_count?.toLocaleString()} students`}
                />
                <KPICard
                  label="Secondary Retention"
                  value={overview.secondary_retention_index}
                  icon={GraduationCap}
                  color={overview.secondary_retention_index >= 0.8 ? "green" : "red"}
                  description="Ages 14-18 / 10-13"
                />
              </div>

              {/* Secondary KPIs */}
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard
                  label="Total Boys"
                  value={overview.total_boys}
                  icon={Users}
                  color="blue"
                />
                <KPICard
                  label="Total Girls"
                  value={overview.total_girls}
                  icon={Users}
                  color="pink"
                />
                <KPICard
                  label="Early Age (4-6)"
                  value={overview.early_age_pct}
                  suffix="%"
                  icon={Baby}
                  color="cyan"
                  description={`${overview.early_age_total?.toLocaleString()} students`}
                />
                <KPICard
                  label="Avg/School"
                  value={overview.avg_students_per_school}
                  icon={School}
                  color="purple"
                />
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Age vs Enrolment Line Chart */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Age-wise Enrolment Trend
                    </CardTitle>
                    <CardDescription>Student distribution by age (4-18 years)</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-72">
                      <ResponsiveContainer width="100%" height="100%">
                        <AreaChart data={ageTrendData}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis dataKey="age" />
                          <YAxis />
                          <Tooltip content={<CustomTooltip />} />
                          <Area type="monotone" dataKey="total" name="Total" fill="#3b82f6" fillOpacity={0.3} stroke="#3b82f6" />
                        </AreaChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>

                {/* Management-wise Enrolment */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Enrolment by School Management
                    </CardTitle>
                    <CardDescription>Top 6 management types</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-72">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={managementPieData}
                            cx="50%"
                            cy="50%"
                            innerRadius={50}
                            outerRadius={90}
                            paddingAngle={2}
                            dataKey="value"
                          >
                            {managementPieData.map((entry, index) => (
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

              {/* Key Insights Row */}
              <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
                <Card className="border-slate-200 bg-blue-50">
                  <CardContent className="p-6 text-center">
                    <Users className="w-10 h-10 mx-auto text-blue-500 mb-3" />
                    <p className="text-3xl font-bold text-blue-700" style={{ fontFamily: 'Manrope' }}>
                      {overview.total_enrolment?.toLocaleString()}
                    </p>
                    <p className="text-sm text-blue-600">Gross Student Enrolment</p>
                    <p className="text-xs text-slate-500 mt-2">{overview.total_blocks} blocks covered</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 bg-emerald-50">
                  <CardContent className="p-6 text-center">
                    <Scale className="w-10 h-10 mx-auto text-emerald-500 mb-3" />
                    <p className="text-3xl font-bold text-emerald-700" style={{ fontFamily: 'Manrope' }}>
                      {overview.gender_parity_index}
                    </p>
                    <p className="text-sm text-emerald-600">Gender Parity Index</p>
                    <p className="text-xs text-slate-500 mt-2">{overview.gender_parity_index >= 0.95 ? "Near gender equity" : "Improvement needed"}</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 bg-amber-50">
                  <CardContent className="p-6 text-center">
                    <TrendingDown className="w-10 h-10 mx-auto text-amber-500 mb-3" />
                    <p className="text-3xl font-bold text-amber-700" style={{ fontFamily: 'Manrope' }}>
                      {overview.secondary_retention_index}
                    </p>
                    <p className="text-sm text-amber-600">Secondary Retention</p>
                    <p className="text-xs text-slate-500 mt-2">31% drop from primary</p>
                  </CardContent>
                </Card>
              </div>
            </TabsContent>

            {/* DASHBOARD 2: AGE & GENDER ANALYSIS */}
            <TabsContent value="age-gender" className="space-y-6">
              {/* Boys vs Girls by Age */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Boys vs Girls by Age
                  </CardTitle>
                  <CardDescription>Gender distribution across age groups</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <ComposedChart data={ageTrendData}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="age" />
                        <YAxis yAxisId="left" />
                        <YAxis yAxisId="right" orientation="right" domain={[0.5, 1.2]} />
                        <Tooltip content={<CustomTooltip />} />
                        <Legend />
                        <Bar yAxisId="left" dataKey="boys" name="Boys" fill="#3b82f6" />
                        <Bar yAxisId="left" dataKey="girls" name="Girls" fill="#ec4899" />
                        <Line yAxisId="right" type="monotone" dataKey="gpi" name="GPI" stroke="#10b981" strokeWidth={2} dot={{ fill: "#10b981" }} />
                      </ComposedChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* GPI Trend Line */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Age-wise Gender Parity Index
                    </CardTitle>
                    <CardDescription>GPI trend across ages (1.0 = parity)</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <LineChart data={ageTrendData}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis dataKey="age" />
                          <YAxis domain={[0.5, 1.1]} />
                          <Tooltip content={<CustomTooltip />} />
                          <Line type="monotone" dataKey="gpi" name="GPI" stroke="#10b981" strokeWidth={2} dot={{ fill: "#10b981", r: 4 }} />
                        </LineChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>

                {/* Age Distribution Table */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Age-wise Enrolment Table
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="overflow-y-auto max-h-64">
                      <Table>
                        <TableHeader>
                          <TableRow className="bg-slate-50">
                            <TableHead>Age</TableHead>
                            <TableHead className="text-right">Boys</TableHead>
                            <TableHead className="text-right">Girls</TableHead>
                            <TableHead className="text-right">Total</TableHead>
                            <TableHead className="text-right">GPI</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {ageData.filter(a => parseInt(a.age) <= 22).map((age) => (
                            <TableRow key={age.age}>
                              <TableCell className="font-medium">{age.age}</TableCell>
                              <TableCell className="text-right tabular-nums text-blue-600">{age.boys?.toLocaleString()}</TableCell>
                              <TableCell className="text-right tabular-nums text-pink-600">{age.girls?.toLocaleString()}</TableCell>
                              <TableCell className="text-right tabular-nums font-bold">{age.total?.toLocaleString()}</TableCell>
                              <TableCell className="text-right">
                                <Badge className={age.gpi >= 0.95 ? "bg-emerald-100 text-emerald-700" : "bg-amber-100 text-amber-700"}>
                                  {age.gpi}
                                </Badge>
                              </TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Retention Analysis */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Enrolment Drop-off Analysis
                  </CardTitle>
                  <CardDescription>Tracking student retention across age groups</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                    <div className="p-4 bg-cyan-50 rounded-lg text-center">
                      <p className="text-xs text-slate-500">Early Childhood (4-6)</p>
                      <p className="text-2xl font-bold text-cyan-700">{overview.early_age_total?.toLocaleString()}</p>
                      <p className="text-sm text-cyan-600">{overview.early_age_pct}% of total</p>
                    </div>
                    <div className="p-4 bg-blue-50 rounded-lg text-center">
                      <p className="text-xs text-slate-500">Primary (10-13)</p>
                      <p className="text-2xl font-bold text-blue-700">{overview.primary_total?.toLocaleString()}</p>
                      <p className="text-sm text-blue-600">Peak enrolment ages</p>
                    </div>
                    <div className="p-4 bg-amber-50 rounded-lg text-center">
                      <p className="text-xs text-slate-500">Secondary (14-18)</p>
                      <p className="text-2xl font-bold text-amber-700">{overview.secondary_total?.toLocaleString()}</p>
                      <p className="text-sm text-amber-600">Retention focus</p>
                    </div>
                    <div className="p-4 bg-red-50 rounded-lg text-center">
                      <p className="text-xs text-slate-500">Retention Index</p>
                      <p className="text-2xl font-bold text-red-700">{overview.secondary_retention_index}</p>
                      <p className="text-sm text-red-600">{Math.round((1 - overview.secondary_retention_index) * 100)}% drop-off</p>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* DASHBOARD 3: GEOGRAPHY & SCHOOLS */}
            <TabsContent value="geography" className="space-y-6">
              {/* Block-wise Enrolment Table */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Block-wise Enrolment Ranking
                  </CardTitle>
                  <CardDescription>All {overview.total_blocks} blocks sorted by enrolment</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow className="bg-slate-50">
                          <TableHead>Rank</TableHead>
                          <TableHead>Block</TableHead>
                          <TableHead className="text-right">Schools</TableHead>
                          <TableHead className="text-right">Boys</TableHead>
                          <TableHead className="text-right">Girls</TableHead>
                          <TableHead className="text-right">Total</TableHead>
                          <TableHead className="text-right">GPI</TableHead>
                          <TableHead className="text-right">Avg/School</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {blockData.map((block) => (
                          <TableRow key={block.block_name} className="hover:bg-slate-50">
                            <TableCell className="text-slate-500">{block.rank}</TableCell>
                            <TableCell className="font-medium">
                              <BlockLink blockCode={block.block_code}>{block.block_name}</BlockLink>
                            </TableCell>
                            <TableCell className="text-right tabular-nums">{block.total_schools}</TableCell>
                            <TableCell className="text-right tabular-nums text-blue-600">{block.total_boys?.toLocaleString()}</TableCell>
                            <TableCell className="text-right tabular-nums text-pink-600">{block.total_girls?.toLocaleString()}</TableCell>
                            <TableCell className="text-right tabular-nums font-bold">{block.total_enrolment?.toLocaleString()}</TableCell>
                            <TableCell className="text-right">
                              <Badge className={block.gender_parity_index >= 0.95 ? "bg-emerald-100 text-emerald-700" : "bg-amber-100 text-amber-700"}>
                                {block.gender_parity_index}
                              </Badge>
                            </TableCell>
                            <TableCell className="text-right tabular-nums">{block.avg_per_school}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Block-wise GPI Heatmap */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Block-wise Gender Parity
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-80">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={blockData.slice(0, 15)} layout="vertical" margin={{ left: 80 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis type="number" domain={[0.8, 1]} />
                          <YAxis dataKey="block_name" type="category" tick={{ fontSize: 11 }} />
                          <Tooltip content={<CustomTooltip />} />
                          <Bar dataKey="gender_parity_index" name="GPI" radius={[0, 4, 4, 0]}>
                            {blockData.slice(0, 15).map((entry, index) => (
                              <Cell key={`cell-${index}`} fill={entry.gender_parity_index >= 0.95 ? "#10b981" : "#f59e0b"} />
                            ))}
                          </Bar>
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>

                {/* School Size Distribution */}
                {schoolSizeData && (
                  <Card className="border-slate-200">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                        School Size Distribution
                      </CardTitle>
                      <CardDescription>Median: {schoolSizeData.median_size} | Avg: {schoolSizeData.avg_size}</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="h-80">
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart data={schoolSizeData.distribution}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                            <XAxis dataKey="band" />
                            <YAxis />
                            <Tooltip content={<CustomTooltip />} />
                            <Bar dataKey="count" name="Schools" fill="#8b5cf6" radius={[4, 4, 0, 0]} />
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                    </CardContent>
                  </Card>
                )}
              </div>

              {/* Top Schools */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Top 10 Schools by Enrolment
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow className="bg-slate-50">
                          <TableHead>#</TableHead>
                          <TableHead>School Name</TableHead>
                          <TableHead>Block</TableHead>
                          <TableHead className="text-right">Boys</TableHead>
                          <TableHead className="text-right">Girls</TableHead>
                          <TableHead className="text-right">Total</TableHead>
                          <TableHead className="text-right">GPI</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {topSchools.slice(0, 10).map((school) => (
                          <TableRow key={school.udise_code}>
                            <TableCell className="text-slate-500">{school.rank}</TableCell>
                            <TableCell className="font-medium max-w-xs truncate">
                              <SchoolLink udiseCode={school.udise_code}>{school.school_name}</SchoolLink>
                            </TableCell>
                            <TableCell>
                              <BlockLink blockCode={school.block_code}>{school.block_name}</BlockLink>
                            </TableCell>
                            <TableCell className="text-right tabular-nums">{school.total_boys?.toLocaleString()}</TableCell>
                            <TableCell className="text-right tabular-nums">{school.total_girls?.toLocaleString()}</TableCell>
                            <TableCell className="text-right tabular-nums font-bold">{school.total_enrolment?.toLocaleString()}</TableCell>
                            <TableCell className="text-right">
                              <Badge className={school.gender_parity_index >= 0.95 ? "bg-emerald-100 text-emerald-700" : "bg-amber-100 text-amber-700"}>
                                {school.gender_parity_index}
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

            {/* DASHBOARD 4: POLICY & MANAGEMENT */}
            <TabsContent value="policy" className="space-y-6">
              {/* Management-wise Table */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Enrolment by School Management
                  </CardTitle>
                  <CardDescription>Public vs Private breakdown</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow className="bg-slate-50">
                          <TableHead>Management Type</TableHead>
                          <TableHead className="text-right">Schools</TableHead>
                          <TableHead className="text-right">Boys</TableHead>
                          <TableHead className="text-right">Girls</TableHead>
                          <TableHead className="text-right">Total</TableHead>
                          <TableHead className="text-right">GPI</TableHead>
                          <TableHead className="text-right">Avg Size</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {managementData.map((mgmt) => (
                          <TableRow key={mgmt.management_code}>
                            <TableCell className="font-medium">{mgmt.management_name}</TableCell>
                            <TableCell className="text-right tabular-nums">{mgmt.total_schools}</TableCell>
                            <TableCell className="text-right tabular-nums text-blue-600">{mgmt.total_boys?.toLocaleString()}</TableCell>
                            <TableCell className="text-right tabular-nums text-pink-600">{mgmt.total_girls?.toLocaleString()}</TableCell>
                            <TableCell className="text-right tabular-nums font-bold">{mgmt.total_enrolment?.toLocaleString()}</TableCell>
                            <TableCell className="text-right">
                              <Badge className={mgmt.gender_parity_index >= 0.95 ? "bg-emerald-100 text-emerald-700" : "bg-amber-100 text-amber-700"}>
                                {mgmt.gender_parity_index}
                              </Badge>
                            </TableCell>
                            <TableCell className="text-right tabular-nums">{mgmt.avg_school_size}</TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Category-wise Enrolment */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Enrolment by School Category
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-72">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={categoryData.slice(0, 8)} layout="vertical" margin={{ left: 120 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis type="number" />
                          <YAxis dataKey="category_name" type="category" tick={{ fontSize: 10 }} width={110} />
                          <Tooltip content={<CustomTooltip />} />
                          <Bar dataKey="total_enrolment" name="Enrolment" fill="#3b82f6" radius={[0, 4, 4, 0]} />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>

                {/* Data Quality */}
                {dataQuality && (
                  <Card className="border-slate-200">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                        Data Quality Metrics
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="space-y-4">
                        <div className="flex justify-between items-center p-3 bg-slate-50 rounded-lg">
                          <span className="text-sm text-slate-600">Total Records</span>
                          <span className="font-bold">{dataQuality.total_records?.toLocaleString()}</span>
                        </div>
                        <div className="flex justify-between items-center p-3 bg-slate-50 rounded-lg">
                          <span className="text-sm text-slate-600">Unique Schools</span>
                          <span className="font-bold">{dataQuality.total_schools?.toLocaleString()}</span>
                        </div>
                        <div className="flex justify-between items-center p-3 bg-emerald-50 rounded-lg">
                          <span className="text-sm text-slate-600">Data Consistency</span>
                          <Badge className={dataQuality.data_consistent ? "bg-emerald-100 text-emerald-700" : "bg-red-100 text-red-700"}>
                            {dataQuality.data_consistent ? "Consistent" : "Issues Found"}
                          </Badge>
                        </div>
                        <div className="flex justify-between items-center p-3 bg-slate-50 rounded-lg">
                          <span className="text-sm text-slate-600">Completeness Score</span>
                          <span className="font-bold text-emerald-600">{dataQuality.completeness_score}%</span>
                        </div>
                        <div className="flex justify-between items-center p-3 bg-amber-50 rounded-lg">
                          <span className="text-sm text-slate-600">Zero Enrolment Records</span>
                          <span className="font-bold text-amber-600">{dataQuality.zero_enrolment_records?.toLocaleString()}</span>
                        </div>
                      </div>
                    </CardContent>
                  </Card>
                )}
              </div>

              {/* Summary Statement */}
              <Card className="border-slate-200 bg-gradient-to-r from-blue-50 to-purple-50">
                <CardContent className="p-6">
                  <h3 className="text-lg font-bold text-slate-800 mb-2">📊 Official Summary</h3>
                  <p className="text-slate-600 italic">
                    "The KPI framework enables monitoring of enrolment coverage ({overview.total_enrolment?.toLocaleString()} students), 
                    gender equity (GPI: {overview.gender_parity_index}), age-wise retention (Secondary Index: {overview.secondary_retention_index}), 
                    school capacity ({overview.avg_students_per_school} avg/school), and institutional balance across {managementData.length} management types 
                    — providing a complete evidence base for education planning and policy intervention."
                  </p>
                </CardContent>
              </Card>
            </TabsContent>
            <TabsContent value="insights" className="space-y-6">
              <AiInsightsTab
                title="Age-wise Enrolment Insights"
                description="AI insights, root cause signals, recommendations, and priority actions for age-wise enrolment."
                generate={buildInsights}
              />
            </TabsContent>
          </Tabs>
        </>
      )}
    </div>
  );
};

export default AgeEnrolmentDashboard;
