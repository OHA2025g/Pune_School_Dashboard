import { useState, useEffect, useCallback, useRef } from "react";
import axios from "axios";
import { useScope } from "@/context/ScopeContext";
import { useNavigate } from "react-router-dom";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Badge } from "@/components/ui/badge";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { RefreshCw, Brain, TrendingDown, Building2, Users, FileCheck, Map, Loader2, AlertTriangle, Sparkles } from "lucide-react";
import { toast } from "sonner";
import ExportPanel from "@/components/ExportPanel";
import PuneTalukaMapSvg from "@/components/PuneTalukaMapSvg";
import { BarChart, Bar, XAxis, YAxis, CartesianGrid, Tooltip, ResponsiveContainer, Cell, PieChart, Pie, Legend, ScatterChart, Scatter, ZAxis } from "recharts";
import { getBackendUrl } from "@/lib/backend";
import { BlockLink } from "@/components/DrilldownLink";
import AiInsightsCard, { CollapsibleInsights } from "@/components/AiInsightsCard";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;

// Color scale for choropleth
const getColor = (value, min, max) => {
  const ratio = (value - min) / (max - min);
  if (ratio >= 0.75) return "#10b981"; // Green
  if (ratio >= 0.5) return "#f59e0b"; // Amber
  if (ratio >= 0.25) return "#f97316"; // Orange
  return "#ef4444"; // Red
};

const RAGBadge = ({ status }) => {
  const colors = {
    green: "bg-emerald-100 text-emerald-700",
    amber: "bg-amber-100 text-amber-700",
    red: "bg-red-100 text-red-700",
    High: "bg-red-100 text-red-700",
    Medium: "bg-amber-100 text-amber-700",
    Low: "bg-emerald-100 text-emerald-700"
  };
  return <Badge className={colors[status] || "bg-slate-100"}>{status}</Badge>;
};

// CollapsibleInsights + AiInsightsCard moved to a shared component so we can reuse
// the same 4 sections (Insights / Root Cause / Recommendations / Priority Actions)
// on District / Block / School drilldowns too.

const BlockMapVisualization = ({ data, selectedMetric, onMetricChange }) => {
  const metrics = [
    { value: "shi_score", label: "School Health Index" },
    { value: "classroom_health", label: "Classroom Health %" },
    { value: "toilet_functional", label: "Toilet Functional %" },
    { value: "apaar_rate", label: "APAAR Rate %" },
    { value: "teacher_quality", label: "Teacher Quality (CTET %)" }
  ];

  const safeNum = (v) => (typeof v === "number" && Number.isFinite(v) ? v : 0);
  const sortedData = [...data].sort((a, b) => safeNum(b?.[selectedMetric]) - safeNum(a?.[selectedMetric]));

  const values = (data || [])
    .map((d) => d?.[selectedMetric])
    .filter((v) => typeof v === "number" && Number.isFinite(v));
  const minVal = values.length ? Math.min(...values) : 0;
  const maxVal = values.length ? Math.max(...values) : 100;

  // Pune block centroids (approx.) in % coordinates relative to the image container.
  // If a block isn't listed, it simply won't get a marker (it still appears in the ranking/table).
  const PUNE_BLOCK_POINTS = {
    "JUNNAR": { x: 52, y: 14 },
    "AMBEGAON": { x: 45, y: 28 },
    "KHED": { x: 36, y: 36 },
    "MAVAL": { x: 20, y: 39 },
    "MULSHI": { x: 23, y: 56 },
    "PUNE CITY": { x: 35, y: 60 },
    "HAVELI": { x: 47, y: 60 },
    "VELHE": { x: 18, y: 75 },
    "BHOR": { x: 34, y: 80 },
    "PURANDHAR": { x: 49, y: 77 },
    "SHIRUR": { x: 62, y: 42 },
    "DAUND": { x: 65, y: 63 },
    "BARAMATI": { x: 60, y: 79 },
    "INDAPUR": { x: 86, y: 83 },
  };

  const normalizeBlockName = (s) =>
    String(s || "")
      .trim()
      .toUpperCase()
      .replace(/\s+/g, " ");

  // Use the SVG outline map (no background image)

  return (
    <div className="space-y-4">
      <div className="flex items-center justify-between">
        <h3 className="font-semibold text-slate-800">Block Performance Map</h3>
        <Select value={selectedMetric} onValueChange={onMetricChange}>
          <SelectTrigger className="w-56">
            <SelectValue />
          </SelectTrigger>
          <SelectContent>
            {metrics.map(m => (
              <SelectItem key={m.value} value={m.value}>{m.label}</SelectItem>
            ))}
          </SelectContent>
        </Select>
      </div>

      <PuneTalukaMapSvg
        // Restrict to the 14 talukas that match the reference map image
        blocks={sortedData.filter((b) => {
          const n = String(b?.block_name || "").trim().toUpperCase();
          return [
            "JUNNAR","AMBEGAON","KHED","MAVAL","MULSHI","VELHE","BHOR",
            "PURANDHAR","PURANDAR","PUNE CITY","HAVELI","SHIRUR","DAUND","BARAMATI","INDAPUR"
          ].includes(n);
        })}
        selectedMetric={selectedMetric}
        getColor={getColor}
        minVal={minVal}
        maxVal={maxVal}
      />

      {/* Legend */}
      <div className="flex items-center justify-center gap-4 pt-2">
        <div className="flex items-center gap-1">
          <div className="w-4 h-4 rounded bg-emerald-500" />
          <span className="text-xs text-slate-600">High (≥75)</span>
        </div>
        <div className="flex items-center gap-1">
          <div className="w-4 h-4 rounded bg-amber-500" />
          <span className="text-xs text-slate-600">Medium (50-75)</span>
        </div>
        <div className="flex items-center gap-1">
          <div className="w-4 h-4 rounded bg-orange-500" />
          <span className="text-xs text-slate-600">Low (25-50)</span>
        </div>
        <div className="flex items-center gap-1">
          <div className="w-4 h-4 rounded bg-red-500" />
          <span className="text-xs text-slate-600">Critical (&lt;25)</span>
        </div>
      </div>
    </div>
  );
};

const AnalyticsDashboard = () => {
  const { scope } = useScope();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [activeTab, setActiveTab] = useState("map");
  const [mapData, setMapData] = useState([]);
  const [selectedMetric, setSelectedMetric] = useState("shi_score");
  
  const [dropoutData, setDropoutData] = useState(null);
  const [infraData, setInfraData] = useState(null);
  const [teacherData, setTeacherData] = useState(null);
  const [completionData, setCompletionData] = useState(null);
  const [executiveInsights, setExecutiveInsights] = useState(null);
  
  const [loadingPrediction, setLoadingPrediction] = useState(null);

  const fetchMapData = useCallback(async () => {
    try {
      const response = await axios.get(`${API}/analytics/map/block-metrics`, {
        params: {
          district_code: scope.districtCode || undefined,
          block_code: scope.blockCode || undefined,
          udise_code: scope.udiseCode || undefined,
        },
      });
      const blocks = response.data.blocks || [];

      // Enrich with real block_code for drilldown navigation.
      // analytics/map/block-metrics does not currently return block_code reliably.
      const districtCode = scope.districtCode || "2725"; // default Pune
      try {
        const scopeRes = await axios.get(`${API}/scope/districts/${districtCode}/blocks`, {
          headers: { "x-skip-scope": "true" },
        });
        const byName = new Map(
          (scopeRes.data || []).map((b) => [String(b.block_name || "").trim().toUpperCase(), b.block_code]),
        );

        const enriched = blocks.map((b) => {
          const key = String(b.block_name || "").trim().toUpperCase();
          return { ...b, block_code: b.block_code || byName.get(key) || null };
        });
        setMapData(enriched);
      } catch {
        setMapData(blocks);
      }
    } catch (error) {
      console.error("Error fetching map data:", error);
    }
  }, [scope.blockCode, scope.districtCode, scope.udiseCode]);

  const fetchPrediction = useCallback(async (type) => {
    setLoadingPrediction(type);
    try {
      const endpoints = {
        dropout: "/analytics/predictions/dropout-risk",
        infrastructure: "/analytics/predictions/infrastructure-forecast",
        teacher: "/analytics/predictions/teacher-shortage",
        completion: "/analytics/predictions/data-completion",
        executive: "/analytics/insights/executive-summary"
      };
      
      const token = localStorage.getItem("token");
      const response = await axios.get(`${API}${endpoints[type]}`, {
        headers: token ? { Authorization: `Bearer ${token}` } : undefined,
        params: {
          district_code: scope.districtCode || undefined,
          block_code: scope.blockCode || undefined,
          udise_code: scope.udiseCode || undefined,
        },
      });
      
      switch (type) {
        case "dropout": setDropoutData(response.data); break;
        case "infrastructure": setInfraData(response.data); break;
        case "teacher": setTeacherData(response.data); break;
        case "completion": setCompletionData(response.data); break;
        case "executive": setExecutiveInsights(response.data); break;
      }
      
      toast.success("AI analysis complete!");
    } catch (error) {
      const status = error?.response?.status;
      const detail =
        error?.response?.data?.detail ||
        error?.message ||
        "Failed to generate prediction";

      // If session is invalid/expired, force re-login so protected endpoints work.
      if (status === 401) {
        localStorage.removeItem("token");
        localStorage.removeItem("user");
        toast.error("Session expired. Please log in again.");
        navigate("/login");
      } else {
        toast.error(detail);
      }
      console.error("Failed to generate prediction:", { type, status, detail, error });
    } finally {
      setLoadingPrediction(null);
    }
  }, [navigate, scope.blockCode, scope.districtCode, scope.udiseCode]);

  useEffect(() => {
    const loadData = async () => {
      setLoading(true);
      await fetchMapData();
      setLoading(false);
    };
    loadData();
  }, [fetchMapData]);

  const didMountRef = useRef(false);
  useEffect(() => {
    if (!didMountRef.current) {
      didMountRef.current = true;
      return;
    }

    const hadDropout = Boolean(dropoutData);
    const hadInfra = Boolean(infraData);
    const hadTeacher = Boolean(teacherData);
    const hadCompletion = Boolean(completionData);
    const hadExecutive = Boolean(executiveInsights);

    setDropoutData(null);
    setInfraData(null);
    setTeacherData(null);
    setCompletionData(null);
    setExecutiveInsights(null);

    const activeGenerated = (
      (activeTab === "dropout" && hadDropout) ||
      (activeTab === "infrastructure" && hadInfra) ||
      (activeTab === "teacher" && hadTeacher) ||
      (activeTab === "completion" && hadCompletion) ||
      (activeTab === "executive" && hadExecutive)
    );

    if (activeTab !== "map" && activeGenerated) {
      fetchPrediction(activeTab);
    }
  }, [
    activeTab,
    fetchPrediction,
    scope.blockCode,
    scope.districtCode,
    scope.udiseCode,
  ]);

  const CustomTooltip = ({ active, payload }) => {
    if (active && payload && payload.length) {
      const data = payload[0].payload;
      return (
        <div className="bg-slate-900 text-white p-3 rounded-lg shadow-lg">
          <p className="font-medium">{data.block || data.block_name}</p>
          {Object.entries(data).filter(([k]) => k !== 'block' && k !== 'block_name').slice(0, 4).map(([key, value]) => (
            <p key={key} className="text-sm text-slate-300">{key}: {typeof value === 'number' ? value.toLocaleString() : value}</p>
          ))}
        </div>
      );
    }
    return null;
  };

  if (loading) {
    return <div className="flex items-center justify-center h-96"><div className="loading-spinner" /></div>;
  }

  return (
    <div className="space-y-6 animate-fade-in" data-testid="analytics-dashboard">
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight flex items-center gap-3" style={{ fontFamily: 'Manrope' }}>
            <Brain className="w-8 h-8 text-purple-600" />
            Advanced Analytics
          </h1>
          <p className="text-slate-500 mt-1">AI-Powered Predictions & Insights • Pune District</p>
        </div>
        <ExportPanel dashboardName="analytics" dashboardTitle="Advanced Analytics" />
          <Button variant="outline" size="sm" onClick={fetchMapData}>
          <RefreshCw className="w-4 h-4 mr-2" />Refresh
        </Button>
      </div>

      <Tabs value={activeTab} onValueChange={setActiveTab} className="space-y-6">
        <TabsList className="bg-slate-100 flex-wrap h-auto gap-1 p-1">
          <TabsTrigger value="map" data-testid="tab-map"><Map className="w-4 h-4 mr-2" />Block Map</TabsTrigger>
          <TabsTrigger value="dropout" data-testid="tab-dropout"><TrendingDown className="w-4 h-4 mr-2" />Dropout Risk</TabsTrigger>
          <TabsTrigger value="infrastructure" data-testid="tab-infra"><Building2 className="w-4 h-4 mr-2" />Infrastructure</TabsTrigger>
          <TabsTrigger value="teacher" data-testid="tab-teacher"><Users className="w-4 h-4 mr-2" />Teacher Forecast</TabsTrigger>
          <TabsTrigger value="completion" data-testid="tab-completion"><FileCheck className="w-4 h-4 mr-2" />Data Completion</TabsTrigger>
          <TabsTrigger value="executive" data-testid="tab-executive"><Sparkles className="w-4 h-4 mr-2" />Executive Insights</TabsTrigger>
        </TabsList>

        {/* MAP TAB */}
        <TabsContent value="map" className="space-y-6">
          <Card className="border-slate-200">
            <CardContent className="p-6">
              <BlockMapVisualization 
                data={mapData} 
                selectedMetric={selectedMetric}
                onMetricChange={setSelectedMetric}
              />
            </CardContent>
          </Card>

          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            <Card className="border-slate-200">
              <CardHeader className="pb-2">
                <CardTitle className="text-lg">Block Performance Ranking</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="h-80">
                  <ResponsiveContainer width="100%" height="100%">
                    <BarChart data={[...mapData].sort((a, b) => b[selectedMetric] - a[selectedMetric]).slice(0, 10)} layout="vertical" margin={{ left: 80 }}>
                      <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                      <XAxis type="number" domain={[0, 100]} />
                      <YAxis dataKey="block_name" type="category" tick={{ fontSize: 11 }} />
                      <Tooltip content={<CustomTooltip />} />
                      <Bar dataKey={selectedMetric} radius={[0, 4, 4, 0]}>
                        {[...mapData].sort((a, b) => b[selectedMetric] - a[selectedMetric]).slice(0, 10).map((entry, index) => (
                          <Cell key={`cell-${index}`} fill={getColor(entry[selectedMetric], 0, 100)} />
                        ))}
                      </Bar>
                    </BarChart>
                  </ResponsiveContainer>
                </div>
              </CardContent>
            </Card>

            <Card className="border-slate-200">
              <CardHeader className="pb-2">
                <CardTitle className="text-lg">Block Details</CardTitle>
              </CardHeader>
              <CardContent>
                <div className="max-h-80 overflow-y-auto">
                  <Table>
                    <TableHeader>
                      <TableRow className="bg-slate-50">
                        <TableHead>Block</TableHead>
                        <TableHead className="text-right">Schools</TableHead>
                        <TableHead className="text-right">SHI</TableHead>
                        <TableHead className="text-center">Status</TableHead>
                      </TableRow>
                    </TableHeader>
                    <TableBody>
                      {[...mapData].sort((a, b) => b.shi_score - a.shi_score).map((block) => (
                        <TableRow key={block.block_name}>
                          <TableCell className="font-medium">{block.block_name}</TableCell>
                          <TableCell className="text-right">{block.schools}</TableCell>
                          <TableCell className="text-right font-bold">{block.shi_score}</TableCell>
                          <TableCell className="text-center"><RAGBadge status={block.rag_status} /></TableCell>
                        </TableRow>
                      ))}
                    </TableBody>
                  </Table>
                </div>
              </CardContent>
            </Card>
          </div>
        </TabsContent>

        {/* DROPOUT RISK TAB */}
        <TabsContent value="dropout" className="space-y-6">
          {!dropoutData ? (
            <Card className="border-slate-200">
              <CardContent className="py-12 text-center">
                <TrendingDown className="w-16 h-16 mx-auto text-slate-300 mb-4" />
                <h3 className="text-xl font-semibold text-slate-700 mb-2">Dropout Risk Analysis</h3>
                <p className="text-slate-500 mb-4">AI-powered analysis of student dropout patterns</p>
                <Button onClick={() => fetchPrediction("dropout")} disabled={loadingPrediction === "dropout"}>
                  {loadingPrediction === "dropout" ? <><Loader2 className="w-4 h-4 mr-2 animate-spin" />Analyzing...</> : <><Brain className="w-4 h-4 mr-2" />Generate Analysis</>}
                </Button>
              </CardContent>
            </Card>
          ) : (
            <>
              <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                <Card className="border-l-4 border-l-blue-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Total Blocks</p><p className="text-2xl font-bold">{dropoutData.summary.total_blocks}</p></CardContent></Card>
                <Card className="border-l-4 border-l-red-500"><CardContent className="p-4"><p className="text-xs text-slate-500">High Risk</p><p className="text-2xl font-bold text-red-600">{dropoutData.summary.high_risk_count}</p></CardContent></Card>
                <Card className="border-l-4 border-l-amber-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Medium Risk</p><p className="text-2xl font-bold text-amber-600">{dropoutData.summary.medium_risk_count}</p></CardContent></Card>
                <Card className="border-l-4 border-l-emerald-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Low Risk</p><p className="text-2xl font-bold text-emerald-600">{dropoutData.summary.low_risk_count}</p></CardContent></Card>
                <Card className="border-l-4 border-l-purple-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Avg Dropout Rate</p><p className="text-2xl font-bold">{dropoutData.summary.avg_dropout_rate}%</p></CardContent></Card>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <AiInsightsCard title="AI-Powered Insights" content={dropoutData.ai_insights} loading={loadingPrediction === "dropout"} />
                
                <Card className="border-slate-200">
                  <CardHeader className="pb-2"><CardTitle className="text-lg">Block Risk Analysis</CardTitle></CardHeader>
                  <CardContent>
                    <div className="max-h-80 overflow-y-auto">
                      <Table>
                        <TableHeader>
                          <TableRow className="bg-slate-50">
                            <TableHead>Block</TableHead>
                            <TableHead className="text-right">Dropout</TableHead>
                            <TableHead className="text-right">Rate</TableHead>
                            <TableHead className="text-center">Risk</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {dropoutData.block_risk_data.slice(0, 15).map((block) => (
                            <TableRow key={block.block}>
                              <TableCell className="font-medium">{block.block}</TableCell>
                              <TableCell className="text-right">{block.dropout_count}</TableCell>
                              <TableCell className="text-right">{block.dropout_rate}%</TableCell>
                              <TableCell className="text-center"><RAGBadge status={block.risk_level} /></TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  </CardContent>
                </Card>
              </div>
            </>
          )}
        </TabsContent>

        {/* INFRASTRUCTURE TAB */}
        <TabsContent value="infrastructure" className="space-y-6">
          {!infraData ? (
            <Card className="border-slate-200">
              <CardContent className="py-12 text-center">
                <Building2 className="w-16 h-16 mx-auto text-slate-300 mb-4" />
                <h3 className="text-xl font-semibold text-slate-700 mb-2">Infrastructure Forecast</h3>
                <p className="text-slate-500 mb-4">AI-powered infrastructure gap analysis & budget forecast</p>
                <Button onClick={() => fetchPrediction("infrastructure")} disabled={loadingPrediction === "infrastructure"}>
                  {loadingPrediction === "infrastructure" ? <><Loader2 className="w-4 h-4 mr-2 animate-spin" />Analyzing...</> : <><Brain className="w-4 h-4 mr-2" />Generate Forecast</>}
                </Button>
              </CardContent>
            </Card>
          ) : (
            <>
              <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                <Card className="border-l-4 border-l-blue-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Total Classrooms</p><p className="text-2xl font-bold">{infraData.summary.total_classrooms.toLocaleString()}</p></CardContent></Card>
                <Card className="border-l-4 border-l-amber-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Current Repair</p><p className="text-2xl font-bold">{infraData.summary.current_repair_needed.toLocaleString()}</p></CardContent></Card>
                <Card className="border-l-4 border-l-orange-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Forecast Repair</p><p className="text-2xl font-bold">{infraData.summary.forecast_repair_needed.toLocaleString()}</p></CardContent></Card>
                <Card className="border-l-4 border-l-red-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Dilapidated</p><p className="text-2xl font-bold text-red-600">{infraData.summary.total_dilapidated}</p></CardContent></Card>
                <Card className="border-l-4 border-l-emerald-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Est. Budget</p><p className="text-2xl font-bold">₹{infraData.summary.estimated_budget_lakhs}L</p></CardContent></Card>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <AiInsightsCard title="AI-Powered Insights" content={infraData.ai_insights} loading={loadingPrediction === "infrastructure"} />
                
                <Card className="border-slate-200">
                  <CardHeader className="pb-2"><CardTitle className="text-lg">Block Infrastructure Status</CardTitle></CardHeader>
                  <CardContent>
                    <div className="max-h-80 overflow-y-auto">
                      <Table>
                        <TableHeader>
                          <TableRow className="bg-slate-50">
                            <TableHead>Block</TableHead>
                            <TableHead className="text-right">Repair %</TableHead>
                            <TableHead className="text-right">Budget (L)</TableHead>
                            <TableHead className="text-center">Priority</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {infraData.block_forecast.slice(0, 15).map((block) => (
                            <TableRow key={block.block}>
                              <TableCell className="font-medium">{block.block}</TableCell>
                              <TableCell className="text-right">{block.repair_rate}%</TableCell>
                              <TableCell className="text-right">₹{block.estimated_budget_lakhs}</TableCell>
                              <TableCell className="text-center"><RAGBadge status={block.priority} /></TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  </CardContent>
                </Card>
              </div>
            </>
          )}
        </TabsContent>

        {/* TEACHER SHORTAGE TAB */}
        <TabsContent value="teacher" className="space-y-6">
          {!teacherData ? (
            <Card className="border-slate-200">
              <CardContent className="py-12 text-center">
                <Users className="w-16 h-16 mx-auto text-slate-300 mb-4" />
                <h3 className="text-xl font-semibold text-slate-700 mb-2">Teacher Shortage Prediction</h3>
                <p className="text-slate-500 mb-4">AI-powered retirement forecast & hiring needs analysis</p>
                <Button onClick={() => fetchPrediction("teacher")} disabled={loadingPrediction === "teacher"}>
                  {loadingPrediction === "teacher" ? <><Loader2 className="w-4 h-4 mr-2 animate-spin" />Analyzing...</> : <><Brain className="w-4 h-4 mr-2" />Generate Forecast</>}
                </Button>
              </CardContent>
            </Card>
          ) : (
            <>
              <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                <Card className="border-l-4 border-l-blue-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Total Teachers</p><p className="text-2xl font-bold">{teacherData.summary.total_teachers.toLocaleString()}</p></CardContent></Card>
                <Card className="border-l-4 border-l-amber-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Retiring (5yr)</p><p className="text-2xl font-bold">{teacherData.summary.retiring_in_5_years.toLocaleString()}</p></CardContent></Card>
                <Card className="border-l-4 border-l-emerald-500"><CardContent className="p-4"><p className="text-xs text-slate-500">New Entrants</p><p className="text-2xl font-bold">{teacherData.summary.new_entrants.toLocaleString()}</p></CardContent></Card>
                <Card className="border-l-4 border-l-red-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Net Shortage</p><p className="text-2xl font-bold text-red-600">{teacherData.summary.net_shortage_5yr.toLocaleString()}</p></CardContent></Card>
                <Card className="border-l-4 border-l-purple-500"><CardContent className="p-4"><p className="text-xs text-slate-500">CTET Rate</p><p className="text-2xl font-bold">{teacherData.summary.avg_ctet_rate}%</p></CardContent></Card>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <AiInsightsCard title="AI-Powered Insights" content={teacherData.ai_insights} loading={loadingPrediction === "teacher"} />
                
                <Card className="border-slate-200">
                  <CardHeader className="pb-2"><CardTitle className="text-lg">Age Distribution</CardTitle></CardHeader>
                  <CardContent>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={Object.entries(teacherData.age_distribution).map(([age, count]) => ({ age, count }))}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis dataKey="age" />
                          <YAxis />
                          <Tooltip />
                          <Bar dataKey="count" fill="#8b5cf6" radius={[4, 4, 0, 0]} />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>
              </div>
            </>
          )}
        </TabsContent>

        {/* DATA COMPLETION TAB */}
        <TabsContent value="completion" className="space-y-6">
          {!completionData ? (
            <Card className="border-slate-200">
              <CardContent className="py-12 text-center">
                <FileCheck className="w-16 h-16 mx-auto text-slate-300 mb-4" />
                <h3 className="text-xl font-semibold text-slate-700 mb-2">Data Completion Timeline</h3>
                <p className="text-slate-500 mb-4">APAAR/Aadhaar completion forecast & acceleration strategy</p>
                <Button onClick={() => fetchPrediction("completion")} disabled={loadingPrediction === "completion"}>
                  {loadingPrediction === "completion" ? <><Loader2 className="w-4 h-4 mr-2 animate-spin" />Analyzing...</> : <><Brain className="w-4 h-4 mr-2" />Generate Timeline</>}
                </Button>
              </CardContent>
            </Card>
          ) : (
            <>
              <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                <Card className="border-l-4 border-l-blue-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Total Students</p><p className="text-2xl font-bold">{(completionData.summary.total_students / 100000).toFixed(1)}L</p></CardContent></Card>
                <Card className="border-l-4 border-l-emerald-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Generated</p><p className="text-2xl font-bold">{(completionData.summary.apaar_generated / 100000).toFixed(1)}L</p></CardContent></Card>
                <Card className="border-l-4 border-l-purple-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Rate</p><p className="text-2xl font-bold">{completionData.summary.overall_rate}%</p></CardContent></Card>
                <Card className="border-l-4 border-l-amber-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Pending</p><p className="text-2xl font-bold">{(completionData.summary.pending / 1000).toFixed(0)}K</p></CardContent></Card>
                <Card className="border-l-4 border-l-cyan-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Est. Weeks</p><p className="text-2xl font-bold">{completionData.summary.estimated_weeks_to_100}</p></CardContent></Card>
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                <AiInsightsCard title="AI-Powered Insights" content={completionData.ai_insights} loading={loadingPrediction === "completion"} />
                
                <Card className="border-slate-200">
                  <CardHeader className="pb-2"><CardTitle className="text-lg">Block Completion Status</CardTitle></CardHeader>
                  <CardContent>
                    <div className="max-h-80 overflow-y-auto">
                      <Table>
                        <TableHeader>
                          <TableRow className="bg-slate-50">
                            <TableHead>Block</TableHead>
                            <TableHead className="text-right">Rate</TableHead>
                            <TableHead className="text-right">Pending</TableHead>
                            <TableHead className="text-right">Est. Weeks</TableHead>
                          </TableRow>
                        </TableHeader>
                        <TableBody>
                          {completionData.block_data.map((block) => (
                            <TableRow key={block.block}>
                              <TableCell className="font-medium">{block.block}</TableCell>
                              <TableCell className="text-right"><Badge className={block.rate >= 90 ? "bg-emerald-100 text-emerald-700" : "bg-amber-100 text-amber-700"}>{block.rate}%</Badge></TableCell>
                              <TableCell className="text-right">{block.pending.toLocaleString()}</TableCell>
                              <TableCell className="text-right">{block.estimated_weeks}</TableCell>
                            </TableRow>
                          ))}
                        </TableBody>
                      </Table>
                    </div>
                  </CardContent>
                </Card>
              </div>
            </>
          )}
        </TabsContent>

        {/* EXECUTIVE INSIGHTS TAB */}
        <TabsContent value="executive" className="space-y-6">
          {!executiveInsights ? (
            <Card className="border-slate-200 bg-gradient-to-br from-purple-50 to-blue-50">
              <CardContent className="py-12 text-center">
                <Sparkles className="w-16 h-16 mx-auto text-purple-400 mb-4" />
                <h3 className="text-xl font-semibold text-slate-700 mb-2">Executive AI Briefing</h3>
                <p className="text-slate-500 mb-4">Comprehensive AI-generated insights for leadership review</p>
                <Button onClick={() => fetchPrediction("executive")} disabled={loadingPrediction === "executive"} className="bg-purple-600 hover:bg-purple-700">
                  {loadingPrediction === "executive" ? <><Loader2 className="w-4 h-4 mr-2 animate-spin" />Generating...</> : <><Sparkles className="w-4 h-4 mr-2" />Generate Briefing</>}
                </Button>
              </CardContent>
            </Card>
          ) : (
            <>
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <Card className="border-l-4 border-l-blue-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Schools</p><p className="text-2xl font-bold">{executiveInsights.metrics.schools.toLocaleString()}</p></CardContent></Card>
                <Card className="border-l-4 border-l-purple-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Students</p><p className="text-2xl font-bold">{(executiveInsights.metrics.students / 100000).toFixed(1)}L</p></CardContent></Card>
                <Card className="border-l-4 border-l-emerald-500"><CardContent className="p-4"><p className="text-xs text-slate-500">Classroom Health</p><p className="text-2xl font-bold">{executiveInsights.metrics.classroom_health}%</p></CardContent></Card>
                <Card className="border-l-4 border-l-cyan-500"><CardContent className="p-4"><p className="text-xs text-slate-500">APAAR Rate</p><p className="text-2xl font-bold">{executiveInsights.metrics.apaar_rate}%</p></CardContent></Card>
              </div>

              <Card className="border-slate-200 bg-gradient-to-br from-purple-50 to-blue-50">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg flex items-center gap-2">
                    <Sparkles className="w-5 h-5 text-purple-600" />
                    AI Executive Briefing
                  </CardTitle>
                  <CardDescription>Generated {new Date(executiveInsights.generated_at).toLocaleString()}</CardDescription>
                </CardHeader>
                <CardContent>
                  <div className="max-h-[600px] overflow-y-auto pr-2">
                    <CollapsibleInsights content={executiveInsights.ai_summary || ""} />
                  </div>
                </CardContent>
              </Card>
            </>
          )}
        </TabsContent>
      </Tabs>
    </div>
  );
};

export default AnalyticsDashboard;
