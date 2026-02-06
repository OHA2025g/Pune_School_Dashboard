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
  Droplets, 
  ShieldCheck,
  AlertTriangle, 
  RefreshCw,
  Upload,
  School,
  Heart,
  Accessibility,
  BookOpen,
  Utensils,
  Trash2,
  Thermometer,
  Stethoscope,
  Activity,
  CheckCircle2,
  XCircle,
  AlertCircle,
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
const KPICard = ({ label, value, suffix = "", icon: Icon, color = "blue", description, size = "default" }) => {
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

// Gauge Bar Component
const GaugeBar = ({ value, label, color = "#10b981", showTarget = false }) => {
  const getColor = (val) => {
    if (val >= 90) return "#10b981";
    if (val >= 70) return "#f59e0b";
    return "#ef4444";
  };
  
  return (
    <div className="space-y-2">
      <div className="flex items-center justify-between">
        <span className="text-sm text-slate-600">{label}</span>
        <span className="text-sm font-bold" style={{ color: color || getColor(value) }}>{value}%</span>
      </div>
      <div className="h-2 bg-slate-100 rounded-full overflow-hidden">
        <div 
          className="h-full rounded-full transition-all duration-500"
          style={{ width: `${Math.min(value, 100)}%`, backgroundColor: color || getColor(value) }}
        />
      </div>
      {showTarget && (
        <div className="flex justify-between text-xs text-slate-400">
          <span>0%</span>
          <span>Target: 100%</span>
        </div>
      )}
    </div>
  );
};

// Status Indicator
const StatusIndicator = ({ status }) => {
  const config = {
    yes: { icon: CheckCircle2, color: "text-emerald-500", bg: "bg-emerald-50" },
    no: { icon: XCircle, color: "text-red-500", bg: "bg-red-50" },
    non_functional: { icon: AlertCircle, color: "text-amber-500", bg: "bg-amber-50" },
  };
  
  const cfg = config[status] || config.no;
  const Icon = cfg.icon;
  
  return (
    <div className={`p-1 rounded ${cfg.bg}`}>
      <Icon className={`w-4 h-4 ${cfg.color}`} />
    </div>
  );
};

const InfrastructureDashboard = () => {
  const { scope } = useScope();
  const [loading, setLoading] = useState(true);
  const [importing, setImporting] = useState(false);
  const [overview, setOverview] = useState(null);
  const [blockData, setBlockData] = useState([]);
  const [waterDistribution, setWaterDistribution] = useState(null);
  const [hygieneDistribution, setHygieneDistribution] = useState(null);
  const [healthMetrics, setHealthMetrics] = useState([]);
  const [inclusionMetrics, setInclusionMetrics] = useState(null);
  const [highRiskSchools, setHighRiskSchools] = useState([]);
  const [bottomBlocks, setBottomBlocks] = useState([]);
  const [activeTab, setActiveTab] = useState("water");

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      const [
        overviewRes,
        blockRes,
        waterRes,
        hygieneRes,
        healthRes,
        inclusionRes,
        riskRes,
        bottomRes
      ] = await Promise.all([
        axios.get(`${API}/infrastructure/overview`),
        axios.get(`${API}/infrastructure/block-wise`),
        axios.get(`${API}/infrastructure/water-distribution`),
        axios.get(`${API}/infrastructure/hygiene-distribution`),
        axios.get(`${API}/infrastructure/health-metrics`),
        axios.get(`${API}/infrastructure/inclusion-metrics`),
        axios.get(`${API}/infrastructure/high-risk-schools`),
        axios.get(`${API}/infrastructure/bottom-blocks`)
      ]);
      
      setOverview(overviewRes.data);
      setBlockData(blockRes.data);
      setWaterDistribution(waterRes.data);
      setHygieneDistribution(hygieneRes.data);
      setHealthMetrics(healthRes.data);
      setInclusionMetrics(inclusionRes.data);
      setHighRiskSchools(riskRes.data);
      setBottomBlocks(bottomRes.data);
    } catch (error) {
      console.error("Error fetching Infrastructure data:", error);
      toast.error("Failed to load infrastructure data");
    } finally {
      setLoading(false);
    }
  }, [scope.version]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handleImport = async () => {
    const url = "https://customer-assets.emergentagent.com/job_7d6cbc1e-b567-4fbc-af84-06fa5107bbd4/artifacts/pynxtgs1_3.%20Drinking_Water_Other_Details_AY_25-26.xlsx";
    
    setImporting(true);
    try {
      await axios.post(`${API}/infrastructure/import?url=${encodeURIComponent(url)}`);
      toast.success("Infrastructure data import started!");
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
        "- No infrastructure data available for the current filters.",
        "",
        "## Root Cause Signals",
        "- Missing or incomplete records in the selected scope.",
        "",
        "## Recommendations",
        "- Refresh data or broaden filters to include valid records.",
        "",
        "## Priority Action Items",
        "- Validate data import for infrastructure analytics.",
      ].join("\n");
    }

    const pct = (v) => (typeof v === "number" ? `${v}%` : "0%");
    const blocks = Array.isArray(blockData) ? blockData : [];
    const worst = [...blocks].sort((a, b) => (a.water_safety_index || 0) - (b.water_safety_index || 0)).slice(0, 3);

    return [
      "## Insights",
      `- Tap water coverage: **${pct(overview.tap_water_coverage)}**, purification: **${pct(overview.purification_coverage)}**.`,
      `- Water safety index: **${pct(overview.water_safety_index)}** with testing coverage **${pct(overview.water_testing_coverage)}**.`,
      worst.length ? `- Lowest water safety blocks: **${worst.map((b) => b.block_name).join(", ")}**.` : "- Lowest water safety blocks: unavailable.",
      "",
      "## Root Cause Signals",
      "- Low purification and water testing coverage signal unsafe drinking water access.",
      "- High risk scores indicate gaps in ramps, libraries, or sanitation facilities.",
      "",
      "## Recommendations",
      "- Prioritize purification and testing equipment in low-performing blocks.",
      "- Allocate maintenance funds to schools with high infrastructure risk scores.",
      "",
      "## Priority Action Items",
      worst.length ? `- Week 1: audit **${worst.map((b) => b.block_name).join(", ")}** to lift water safety above **${pct(overview.water_safety_index)}**.` : "- Week 1: audit lowest water safety blocks and lift index.",
      `- Week 2: raise purification (**${pct(overview.purification_coverage)}**) + testing (**${pct(overview.water_testing_coverage)}**) coverage.`,
      "- Week 3–4: close high-risk repairs and re-score block risk index.",
    ].join("\n");
  };

  return (
    <div className="space-y-6 animate-fade-in" data-testid="infrastructure-dashboard">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight" style={{ fontFamily: 'Manrope' }}>
            Infrastructure & Water Safety Dashboard
          </h1>
          <p className="text-slate-500 mt-1">Drinking Water & Facilities • AY 2025-26 • Pune District</p>
        </div>
        <div className="flex items-center gap-3">
          <Button 
            variant="outline" 
            size="sm" 
            onClick={handleImport}
            disabled={importing}
            data-testid="import-infra-btn"
          >
            {importing ? <RefreshCw className="w-4 h-4 mr-2 animate-spin" /> : <Upload className="w-4 h-4 mr-2" />}
            {importing ? "Importing..." : "Import Data"}
          </Button>
          <ExportPanel dashboardName="infrastructure" dashboardTitle="Infrastructure" />
          <Button variant="outline" size="sm" onClick={fetchData} data-testid="refresh-btn">
            <RefreshCw className="w-4 h-4 mr-2" />
            Refresh
          </Button>
        </div>
      </div>

      {!hasData ? (
        <Card className="border-slate-200">
          <CardContent className="py-12 text-center">
            <Droplets className="w-16 h-16 mx-auto text-slate-300 mb-4" />
            <h3 className="text-xl font-semibold text-slate-700 mb-2">No Infrastructure Data Available</h3>
            <p className="text-slate-500 mb-4">Click "Import Data" to load the Drinking Water & Facilities Excel file</p>
            <Button onClick={handleImport} disabled={importing} data-testid="import-infra-empty-btn">
              {importing ? "Importing..." : "Import Infrastructure Data"}
            </Button>
          </CardContent>
        </Card>
      ) : (
        <>
          {/* Dashboard Tabs */}
          <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
            <TabsList className="bg-slate-100">
              <TabsTrigger value="water" className="flex items-center gap-2" data-testid="tab-water">
                <Droplets className="w-4 h-4" />
                Water & Safety
              </TabsTrigger>
              <TabsTrigger value="hygiene" className="flex items-center gap-2" data-testid="tab-hygiene">
                <Trash2 className="w-4 h-4" />
                Hygiene & MDM
              </TabsTrigger>
              <TabsTrigger value="health" className="flex items-center gap-2" data-testid="tab-health">
                <Heart className="w-4 h-4" />
                Health & Safety
              </TabsTrigger>
              <TabsTrigger value="inclusion" className="flex items-center gap-2" data-testid="tab-inclusion">
                <Accessibility className="w-4 h-4" />
                Inclusion & Academics
              </TabsTrigger>
              <TabsTrigger value="insights" className="flex items-center gap-2" data-testid="tab-insights">
                <Brain className="w-4 h-4" />
                Insights
              </TabsTrigger>
            </TabsList>

            {/* DASHBOARD 1: WATER & SAFETY */}
            <TabsContent value="water" className="space-y-6">
              {/* Top KPI Strip */}
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-6 gap-4">
                <KPICard
                  label="Total Schools"
                  value={overview.total_schools}
                  icon={School}
                  color="blue"
                />
                <KPICard
                  label="Tap Water"
                  value={overview.tap_water_coverage}
                  suffix="%"
                  icon={Droplets}
                  color={overview.tap_water_coverage >= 90 ? "green" : overview.tap_water_coverage >= 70 ? "amber" : "red"}
                />
                <KPICard
                  label="Purification"
                  value={overview.purification_coverage}
                  suffix="%"
                  icon={ShieldCheck}
                  color={overview.purification_coverage >= 90 ? "green" : overview.purification_coverage >= 70 ? "amber" : "red"}
                />
                <KPICard
                  label="Water Testing"
                  value={overview.water_testing_coverage}
                  suffix="%"
                  icon={Activity}
                  color={overview.water_testing_coverage >= 90 ? "green" : overview.water_testing_coverage >= 70 ? "amber" : "red"}
                />
                <KPICard
                  label="Rainwater Harv."
                  value={overview.rwh_coverage}
                  suffix="%"
                  icon={Droplets}
                  color="cyan"
                />
                <KPICard
                  label="Water Safety Index"
                  value={overview.water_safety_index}
                  icon={ShieldCheck}
                  color={overview.water_safety_index >= 80 ? "green" : "amber"}
                  description="Composite Score"
                />
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Tap Water Distribution */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Tap Water Availability
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={waterDistribution?.tap_water || []}
                            cx="50%"
                            cy="50%"
                            innerRadius={50}
                            outerRadius={80}
                            paddingAngle={2}
                            dataKey="value"
                            label={({ name, value }) => `${name}: ${value}`}
                          >
                            {(waterDistribution?.tap_water || []).map((entry, index) => (
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

                {/* Purification Distribution */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Water Purification Status
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={waterDistribution?.purification || []}
                            cx="50%"
                            cy="50%"
                            innerRadius={50}
                            outerRadius={80}
                            paddingAngle={2}
                            dataKey="value"
                            label={({ name, value }) => `${value}`}
                          >
                            {(waterDistribution?.purification || []).map((entry, index) => (
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

              {/* Block-wise Water Safety */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Block-wise Water Safety Index
                  </CardTitle>
                  <CardDescription>Water Safety = (Tap Water + Purification + Testing) / 3</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="h-80">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={[...blockData].sort((a, b) => b.water_safety_index - a.water_safety_index).slice(0, 15)} layout="vertical" margin={{ left: 80 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis type="number" domain={[0, 100]} />
                        <YAxis dataKey="block_name" type="category" width={70} tick={{ fontSize: 11 }} />
                        <Tooltip content={<CustomTooltip />} />
                        <Bar dataKey="water_safety_index" name="Water Safety Index" radius={[0, 4, 4, 0]}>
                          {[...blockData].sort((a, b) => b.water_safety_index - a.water_safety_index).slice(0, 15).map((entry, index) => (
                            <Cell 
                              key={`cell-${index}`} 
                              fill={entry.water_safety_index >= 90 ? "#10b981" : entry.water_safety_index >= 70 ? "#f59e0b" : "#ef4444"} 
                            />
                          ))}
                        </Bar>
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>

              {/* Bottom 10 Blocks */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                    <AlertTriangle className="w-5 h-5 text-red-500" />
                    Bottom 10 Blocks - Water Safety
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="space-y-3">
                    {bottomBlocks.slice(0, 10).map((block, idx) => (
                      <div key={idx} className="flex items-center justify-between p-3 bg-red-50 rounded-lg">
                        <div className="flex items-center gap-3">
                          <span className="w-6 h-6 flex items-center justify-center bg-red-100 text-red-700 text-xs font-bold rounded-full">
                            {idx + 1}
                          </span>
                          <div>
                            <p className="font-medium text-slate-900">{block.block_name}</p>
                            <p className="text-xs text-slate-500">{block.total_schools} schools</p>
                          </div>
                        </div>
                        <Badge className="bg-red-100 text-red-700">{block.water_safety_index}%</Badge>
                      </div>
                    ))}
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* DASHBOARD 2: HYGIENE & MDM */}
            <TabsContent value="hygiene" className="space-y-6">
              {/* Hygiene KPIs */}
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
                <Card className="border-slate-200 border-l-4 border-l-emerald-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Classroom Dustbin</p>
                    <p className="text-2xl font-bold text-emerald-600">{overview.classroom_hygiene_coverage}%</p>
                    <p className="text-sm text-slate-600">{overview.classroom_dustbin_all} schools (all)</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-blue-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Toilet Dustbin</p>
                    <p className="text-2xl font-bold text-blue-600">{overview.toilet_hygiene_coverage}%</p>
                    <p className="text-sm text-slate-600">{overview.toilet_dustbin_yes} schools</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-purple-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Kitchen Dustbin</p>
                    <p className="text-2xl font-bold text-purple-600">{overview.kitchen_hygiene_coverage}%</p>
                    <p className="text-sm text-slate-600">{overview.kitchen_dustbin_yes} schools</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-amber-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Kitchen Shed</p>
                    <p className="text-2xl font-bold text-amber-600">{overview.kitchen_shed_coverage}%</p>
                    <p className="text-sm text-slate-600">{overview.kitchen_shed_yes} schools</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-cyan-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Kitchen Garden</p>
                    <p className="text-2xl font-bold text-cyan-600">{overview.kitchen_garden_coverage}%</p>
                    <p className="text-sm text-slate-600">{overview.kitchen_garden_yes} schools</p>
                  </CardContent>
                </Card>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Classroom Dustbin Distribution */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Classroom Dustbin Distribution
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={hygieneDistribution?.classroom || []}
                            cx="50%"
                            cy="50%"
                            innerRadius={50}
                            outerRadius={80}
                            paddingAngle={2}
                            dataKey="value"
                            label={({ name, value }) => `${value}`}
                          >
                            {(hygieneDistribution?.classroom || []).map((entry, index) => (
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

                {/* Block-wise Hygiene Coverage */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Block-wise Toilet Hygiene Compliance
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={blockData.slice(0, 12)} layout="vertical" margin={{ left: 80 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis type="number" domain={[0, 100]} />
                          <YAxis dataKey="block_name" type="category" width={70} tick={{ fontSize: 11 }} />
                          <Tooltip content={<CustomTooltip />} />
                          <Bar dataKey="toilet_hygiene_pct" name="Toilet Hygiene %" fill="#3b82f6" radius={[0, 4, 4, 0]} />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* MDM Infrastructure */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Block-wise Kitchen Infrastructure
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="h-72">
                    <ResponsiveContainer width="100%" height="100%">
                      <BarChart data={blockData.slice(0, 15)} margin={{ left: 10 }}>
                        <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                        <XAxis dataKey="block_name" tick={{ fontSize: 10 }} angle={-45} textAnchor="end" height={60} />
                        <YAxis domain={[0, 100]} />
                        <Tooltip content={<CustomTooltip />} />
                        <Legend />
                        <Bar dataKey="kitchen_shed_pct" name="Kitchen Shed %" fill="#f59e0b" radius={[4, 4, 0, 0]} />
                      </BarChart>
                    </ResponsiveContainer>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* DASHBOARD 3: HEALTH & SAFETY */}
            <TabsContent value="health" className="space-y-6">
              {/* Health KPIs */}
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
                <Card className="border-slate-200 border-l-4 border-l-blue-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Medical Check-up</p>
                    <p className="text-2xl font-bold text-blue-600">{overview.medical_checkup_coverage}%</p>
                    <p className="text-sm text-slate-600">{overview.medical_checkup_yes} schools</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-purple-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Health Records</p>
                    <p className="text-2xl font-bold text-purple-600">{overview.health_record_coverage}%</p>
                    <p className="text-sm text-slate-600">{overview.health_record_yes} schools</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-emerald-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">First Aid</p>
                    <p className="text-2xl font-bold text-emerald-600">{overview.first_aid_coverage}%</p>
                    <p className="text-sm text-slate-600">{overview.first_aid_yes} schools</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-amber-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Life-Saving Equip</p>
                    <p className="text-2xl font-bold text-amber-600">{overview.life_saving_coverage}%</p>
                    <p className="text-sm text-slate-600">{overview.life_saving_yes} schools</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-cyan-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Thermal Screening</p>
                    <p className="text-2xl font-bold text-cyan-600">{overview.thermal_coverage}%</p>
                    <p className="text-sm text-slate-600">{overview.thermal_yes} schools</p>
                  </CardContent>
                </Card>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Health Metrics Gauges */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Health & Safety Compliance
                    </CardTitle>
                  </CardHeader>
                  <CardContent className="space-y-5">
                    {healthMetrics.map((metric, idx) => (
                      <GaugeBar 
                        key={idx} 
                        value={metric.percentage} 
                        label={`${metric.name} (${metric.count.toLocaleString()} schools)`} 
                        color={metric.color} 
                      />
                    ))}
                  </CardContent>
                </Card>

                {/* Block-wise First Aid */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Block-wise Emergency Preparedness
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={blockData.slice(0, 12)} layout="vertical" margin={{ left: 80 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis type="number" domain={[0, 100]} />
                          <YAxis dataKey="block_name" type="category" width={70} tick={{ fontSize: 11 }} />
                          <Tooltip content={<CustomTooltip />} />
                          <Bar dataKey="first_aid_pct" name="First Aid %" fill="#10b981" radius={[0, 4, 4, 0]} />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* High Risk Schools */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
                    <AlertTriangle className="w-5 h-5 text-red-500" />
                    High-Risk Schools (Multiple Infrastructure Gaps)
                  </CardTitle>
                  <CardDescription>Schools missing 3+ critical facilities</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow className="bg-slate-50">
                          <TableHead className="font-medium">School Name</TableHead>
                          <TableHead className="font-medium">Block</TableHead>
                          <TableHead className="font-medium text-center">Risk Score</TableHead>
                          <TableHead className="font-medium text-center">Tap Water</TableHead>
                          <TableHead className="font-medium text-center">Purification</TableHead>
                          <TableHead className="font-medium text-center">Testing</TableHead>
                          <TableHead className="font-medium text-center">Kitchen Shed</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {highRiskSchools.slice(0, 15).map((school, idx) => (
                          <TableRow key={idx} className="hover:bg-slate-50">
                            <TableCell className="max-w-xs truncate text-sm">
                              <SchoolLink udiseCode={school.udise_code}>{school.school_name}</SchoolLink>
                            </TableCell>
                            <TableCell className="text-sm">
                              <BlockLink blockCode={school.block_code}>{school.block_name}</BlockLink>
                            </TableCell>
                            <TableCell className="text-center">
                              <Badge className="bg-red-100 text-red-700">{school.risk_count}</Badge>
                            </TableCell>
                            <TableCell className="text-center"><StatusIndicator status={school.tap_water} /></TableCell>
                            <TableCell className="text-center"><StatusIndicator status={school.purification} /></TableCell>
                            <TableCell className="text-center"><StatusIndicator status={school.testing} /></TableCell>
                            <TableCell className="text-center"><StatusIndicator status={school.kitchen_shed} /></TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>

            {/* DASHBOARD 4: INCLUSION & ACADEMICS */}
            <TabsContent value="inclusion" className="space-y-6">
              {/* Inclusion KPIs */}
              <div className="grid grid-cols-2 md:grid-cols-3 lg:grid-cols-5 gap-4">
                <Card className="border-slate-200 border-l-4 border-l-emerald-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Ramp Available</p>
                    <p className="text-2xl font-bold text-emerald-600">{overview.ramp_coverage}%</p>
                    <p className="text-sm text-slate-600">{overview.ramp_yes} schools</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-blue-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Dedicated Educator</p>
                    <p className="text-2xl font-bold text-blue-600">{overview.special_educator_dedicated_pct}%</p>
                    <p className="text-sm text-slate-600">{overview.special_educator_dedicated} schools</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-red-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Inclusion Gap</p>
                    <p className="text-2xl font-bold text-red-600">{overview.inclusion_gap_pct}%</p>
                    <p className="text-sm text-slate-600">{overview.special_educator_no} schools</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-purple-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Library Coverage</p>
                    <p className="text-2xl font-bold text-purple-600">{overview.library_coverage}%</p>
                    <p className="text-sm text-slate-600">{overview.library_yes} schools</p>
                  </CardContent>
                </Card>
                <Card className="border-slate-200 border-l-4 border-l-amber-500">
                  <CardContent className="p-4">
                    <p className="text-xs text-slate-500 uppercase">Avg Books/School</p>
                    <p className="text-2xl font-bold text-amber-600">{overview.avg_books_per_school}</p>
                    <p className="text-sm text-slate-600">{overview.total_books?.toLocaleString()} total</p>
                  </CardContent>
                </Card>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Ramp Distribution */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Physical Accessibility (Ramp)
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={inclusionMetrics?.ramp || []}
                            cx="50%"
                            cy="50%"
                            innerRadius={50}
                            outerRadius={80}
                            paddingAngle={2}
                            dataKey="value"
                            label={({ name, value }) => `${name}: ${value}`}
                          >
                            {(inclusionMetrics?.ramp || []).map((entry, index) => (
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

                {/* Special Educator Distribution */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Special Educator Availability
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={inclusionMetrics?.special_educator || []}
                            cx="50%"
                            cy="50%"
                            innerRadius={50}
                            outerRadius={80}
                            paddingAngle={2}
                            dataKey="value"
                            label={({ name, value }) => `${value}`}
                          >
                            {(inclusionMetrics?.special_educator || []).map((entry, index) => (
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

              {/* Block-wise Metrics Table */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Block-wise Infrastructure Summary
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow className="bg-slate-50">
                          <TableHead className="font-medium">Block</TableHead>
                          <TableHead className="font-medium text-right">Schools</TableHead>
                          <TableHead className="font-medium text-right">Water Safety</TableHead>
                          <TableHead className="font-medium text-right">Tap Water</TableHead>
                          <TableHead className="font-medium text-right">Purification</TableHead>
                          <TableHead className="font-medium text-right">Ramp</TableHead>
                          <TableHead className="font-medium text-right">Library</TableHead>
                          <TableHead className="font-medium text-right">Risk Score</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {blockData.slice(0, 15).map((block, idx) => (
                          <TableRow key={idx} className="hover:bg-slate-50">
                            <TableCell className="font-medium">
                              <BlockLink blockCode={block.block_code}>{block.block_name}</BlockLink>
                            </TableCell>
                            <TableCell className="text-right tabular-nums">{block.total_schools}</TableCell>
                            <TableCell className="text-right">
                              <Badge className={block.water_safety_index >= 80 ? "bg-emerald-100 text-emerald-700" : block.water_safety_index >= 60 ? "bg-amber-100 text-amber-700" : "bg-red-100 text-red-700"}>
                                {block.water_safety_index}%
                              </Badge>
                            </TableCell>
                            <TableCell className="text-right tabular-nums">{block.tap_water_pct}%</TableCell>
                            <TableCell className="text-right tabular-nums">{block.purification_pct}%</TableCell>
                            <TableCell className="text-right tabular-nums">{block.ramp_pct}%</TableCell>
                            <TableCell className="text-right tabular-nums">{block.library_pct}%</TableCell>
                            <TableCell className="text-right">
                              <Badge className={block.risk_score === 0 ? "bg-emerald-100 text-emerald-700" : block.risk_score <= 2 ? "bg-amber-100 text-amber-700" : "bg-red-100 text-red-700"}>
                                {block.risk_score}
                              </Badge>
                            </TableCell>
                          </TableRow>
                        ))}
                      </TableBody>
                    </Table>
                  </div>
                </CardContent>
              </Card>

              {/* Compliance Score Card */}
              <Card className="border-slate-200 bg-gradient-to-r from-blue-50 to-emerald-50">
                <CardContent className="p-6">
                  <div className="flex items-center justify-between">
                    <div>
                      <p className="text-sm text-slate-600 uppercase font-medium">Overall Compliance Score</p>
                      <p className="text-4xl font-bold text-slate-900 mt-2" style={{ fontFamily: 'Manrope' }}>{overview.compliance_score}%</p>
                      <p className="text-sm text-slate-500 mt-1">Based on 8 mandatory infrastructure indicators</p>
                    </div>
                    <div className="w-24 h-24 rounded-full border-8 border-emerald-500 flex items-center justify-center bg-white">
                      <span className="text-2xl font-bold text-emerald-600">{overview.compliance_score >= 80 ? 'A' : overview.compliance_score >= 60 ? 'B' : 'C'}</span>
                    </div>
                  </div>
                </CardContent>
              </Card>
            </TabsContent>
            <TabsContent value="insights" className="space-y-6">
              <AiInsightsTab
                title="Infrastructure & Water Safety Insights"
                description="AI insights, root cause signals, recommendations, and priority actions for infrastructure."
                generate={buildInsights}
              />
            </TabsContent>
          </Tabs>
        </>
      )}
    </div>
  );
};

export default InfrastructureDashboard;
