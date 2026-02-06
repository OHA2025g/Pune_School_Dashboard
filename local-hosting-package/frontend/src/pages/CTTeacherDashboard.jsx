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
  BarChart3,
  Target,
  Scale,
  GraduationCap,
  Award,
  Clock,
  ShieldCheck,
  AlertTriangle,
  CheckCircle2,
  UserCheck,
  Brain
} from "lucide-react";
import { toast } from "sonner";
import ExportPanel from "@/components/ExportPanel";
import AiInsightsTab from "@/components/AiInsightsTab";
import { BlockLink } from "@/components/DrilldownLink";
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
    if (val >= 90) return "#10b981";
    if (val >= 70) return "#f59e0b";
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

const CTTeacherDashboard = () => {
  const { scope } = useScope();
  const [loading, setLoading] = useState(true);
  const [importing, setImporting] = useState(false);
  const [overview, setOverview] = useState(null);
  const [blockData, setBlockData] = useState([]);
  const [genderData, setGenderData] = useState([]);
  const [socialCategory, setSocialCategory] = useState([]);
  const [qualification, setQualification] = useState(null);
  const [ageData, setAgeData] = useState(null);
  const [tenureData, setTenureData] = useState(null);
  const [trainingData, setTrainingData] = useState([]);
  const [dataQuality, setDataQuality] = useState(null);
  const [certification, setCertification] = useState(null);
  const [activeTab, setActiveTab] = useState("executive");

  const fetchData = useCallback(async () => {
    setLoading(true);
    try {
      // The axios interceptor in App.js automatically adds scope parameters from localStorage
      // We can optionally pass them explicitly here, but the interceptor will merge them
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

      console.log('Fetching CTTeacher data with scope:', {
        districtCode: scope.districtCode,
        blockCode: scope.blockCode,
        udiseCode: scope.udiseCode,
        params: requestParams,
        apiUrl: API
      });

      // Make API calls - the interceptor will merge scope params from localStorage
      const [
        overviewRes,
        blockRes,
        genderRes,
        socialRes,
        qualRes,
        ageRes,
        tenureRes,
        trainingRes,
        qualityRes,
        certRes
      ] = await Promise.all([
        axios.get(`${API}/ctteacher/overview`, { params: requestParams }),
        axios.get(`${API}/ctteacher/block-wise`, { params: requestParams }),
        axios.get(`${API}/ctteacher/gender-distribution`, { params: requestParams }),
        axios.get(`${API}/ctteacher/social-category`, { params: requestParams }),
        axios.get(`${API}/ctteacher/qualification`, { params: requestParams }),
        axios.get(`${API}/ctteacher/age-distribution`, { params: requestParams }),
        axios.get(`${API}/ctteacher/service-tenure`, { params: requestParams }),
        axios.get(`${API}/ctteacher/training-demand`, { params: requestParams }),
        axios.get(`${API}/ctteacher/data-quality`, { params: requestParams }),
        axios.get(`${API}/ctteacher/certification`, { params: requestParams })
      ]);
      
      // Log successful fetch
      const overviewData = overviewRes?.data || {};
      console.log('CTTeacher data fetched successfully:', {
        overviewTotalTeachers: overviewData.total_teachers,
        overviewTotalSchools: overviewData.total_schools,
        blockDataCount: Array.isArray(blockRes?.data) ? blockRes.data.length : 0,
        hasGenderData: Array.isArray(genderRes?.data) && genderRes.data.length > 0,
        hasQualification: !!qualRes?.data,
        hasAgeData: !!ageRes?.data
      });
      
      // Set overview data - use the API response directly, with fallbacks for missing fields
      if (overviewData && typeof overviewData === 'object') {
        setOverview(overviewData);
      } else {
        // Fallback: create empty overview object
        setOverview({
          total_teachers: 0,
          unique_teachers: 0,
          total_schools: 0,
          total_blocks: 0,
          avg_teachers_per_school: 0,
          male_count: 0,
          female_count: 0,
          female_pct: 0,
          gender_parity_index: 0,
          aadhaar_verified: 0,
          aadhaar_verified_pct: 0,
          completed: 0,
          completion_pct: 0,
          ctet_qualified: 0,
          ctet_pct: 0,
          nishtha_completed: 0,
          nishtha_pct: 0
        });
      }
      
      // Set other data arrays/objects
      setBlockData(Array.isArray(blockRes?.data) ? blockRes.data : []);
      setGenderData(Array.isArray(genderRes?.data) ? genderRes.data : []);
      setSocialCategory(Array.isArray(socialRes?.data) ? socialRes.data : []);
      setQualification(qualRes?.data || null);
      setAgeData(ageRes?.data || null);
      setTenureData(tenureRes?.data || null);
      setTrainingData(Array.isArray(trainingRes?.data) ? trainingRes.data : []);
      setDataQuality(qualityRes?.data || null);
      setCertification(certRes?.data || null);
    } catch (error) {
      console.error("Error fetching CTTeacher data:", error);
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
        // Don't reset data on network errors - keep existing data visible
        setLoading(false);
        return;
      }
      
      const errorMsg = error.response?.data?.detail || error.message || "Failed to load teacher data";
      
      // Only show error toast for actual errors, not for empty results
      if (error.response?.status && error.response?.status !== 404 && error.response?.status >= 500) {
        toast.error(errorMsg);
      }
      
      // Reset data on error to show "No Data" message (but not on network errors)
      if (error.response) {
        setOverview(null);
        setBlockData([]);
        setGenderData([]);
        setSocialCategory([]);
        setQualification(null);
        setAgeData(null);
        setTenureData(null);
        setTrainingData([]);
        setDataQuality(null);
        setCertification(null);
      }
    } finally {
      setLoading(false);
    }
  }, [scope.version, scope.districtCode, scope.blockCode, scope.udiseCode]);

  useEffect(() => {
    fetchData();
  }, [fetchData]);

  const handleImport = async () => {
    const url = "https://customer-assets.emergentagent.com/job_e600aca7-d1b5-4003-a850-c6b4b2f65c48/artifacts/7h74ajig_8.%20CTTeacher%20Data%202025-26.xlsx";
    
    setImporting(true);
    try {
      await axios.post(`${API}/ctteacher/import?url=${encodeURIComponent(url)}`);
      toast.success("CTTeacher data import started!");
      setTimeout(() => {
        fetchData();
        setImporting(false);
      }, 30000); // Large file needs more time to process
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

  const buildInsights = useCallback(() => {
    if (!overview) {
      return [
        "## Insights",
        "- No CTTeacher data available for the current filters.",
        "",
        "## Root Cause Signals",
        "- Missing or incomplete records in the selected scope.",
        "",
        "## Recommendations",
        "- Refresh data or broaden filters to include valid records.",
        "",
        "## Priority Action Items",
        "- Validate data import for CTTeacher analytics.",
      ].join("\n");
    }

    const fmt = (v) => (typeof v === "number" ? v.toLocaleString("en-IN") : v ?? 0);
    const pct = (v) => (typeof v === "number" ? `${v}%` : "0%");

    // Get scope context
    const scopeLabel = scope.block_name 
      ? `Block: ${scope.block_name}` 
      : scope.school_name 
      ? `School: ${scope.school_name}` 
      : scope.district_name 
      ? `District: ${scope.district_name}` 
      : "Pune District";

    // Ensure blockData is an array
    const blocks = Array.isArray(blockData) ? blockData : [];
    
    // Identify top and bottom performing blocks within current scope (using available metrics)
    const sortedBlocks = [...blocks].sort((a, b) => {
      const scoreA = (a.aadhaar_pct || 0) + (a.female_pct || 0) + (a.avg_per_school || 0);
      const scoreB = (b.aadhaar_pct || 0) + (b.female_pct || 0) + (b.avg_per_school || 0);
      return scoreB - scoreA;
    });
    const topBlock = sortedBlocks[0];
    const bottomBlock = sortedBlocks[sortedBlocks.length - 1];

    // Calculate averages for comparison (using overview data since block data doesn't have CTET/completion)
    const avgCtet = overview.ctet_pct || 0;
    const avgAadhaar = blocks.length > 0 
      ? blocks.reduce((sum, b) => sum + (b.aadhaar_pct || 0), 0) / blocks.length 
      : overview.aadhaar_verified_pct || 0;
    const avgCompletion = overview.completion_pct || 0;

    // Identify specific issues (only using available block metrics)
    const lowAadhaarBlocks = blocks.filter(b => (b.aadhaar_pct || 0) < 80).slice(0, 3);

    // Use unique_teachers as the primary count, with total_teachers as records if different
    const teacherCount = overview.unique_teachers || overview.total_teachers;
    const teacherCountNote = overview.unique_teachers && overview.unique_teachers !== overview.total_teachers
      ? ` (${fmt(overview.total_teachers)} records)`
      : '';
    
    return [
      "## Insights",
      `- **Scope**: ${scopeLabel}`,
      `- Total teachers: **${fmt(teacherCount)}**${teacherCountNote} across **${fmt(overview.total_schools)}** schools${scope.block_name ? ` in ${scope.block_name}` : scope.school_name ? ` at ${scope.school_name}` : ""}.`,
      `- Data completion: **${pct(overview.completion_pct)}** (avg: ${pct(avgCompletion)}), Aadhaar verified: **${pct(overview.aadhaar_verified_pct)}** (avg: ${pct(avgAadhaar)}).`,
      `- CTET qualified: **${pct(overview.ctet_pct)}** (avg: ${pct(avgCtet)}), NISHTHA completed: **${pct(overview.nishtha_pct)}**.`,
      `- Gender parity index: **${overview.gender_parity_index}**${overview.gender_parity_index < 0.8 ? " (below target)" : ""}.`,
      topBlock && bottomBlock && topBlock !== bottomBlock 
        ? `- Top performing block: **${topBlock.block_name}** (Aadhaar: ${pct(topBlock.aadhaar_pct)}, Teachers: ${fmt(topBlock.total_teachers)}).`
        : "",
      topBlock && bottomBlock && topBlock !== bottomBlock 
        ? `- Bottom performing block: **${bottomBlock.block_name}** (Aadhaar: ${pct(bottomBlock.aadhaar_pct)}, Teachers: ${fmt(bottomBlock.total_teachers)}).`
        : "",
      "",
      "## Root Cause Signals",
      overview.ctet_pct < avgCtet 
        ? `- CTET qualification rate (${pct(overview.ctet_pct)}) is below average (${pct(avgCtet)}), indicating training gaps${scope.block_name ? ` in ${scope.block_name}` : scope.school_name ? ` at ${scope.school_name}` : " in select blocks"}.`
        : `- CTET qualification rate (${pct(overview.ctet_pct)}) is ${overview.ctet_pct >= 70 ? "above" : "at"} average.`,
      overview.aadhaar_verified_pct < avgAadhaar 
        ? `- Aadhaar verification (${pct(overview.aadhaar_verified_pct)}) is below average (${pct(avgAadhaar)}), suggesting documentation delays${scope.block_name ? ` in ${scope.block_name}` : scope.school_name ? ` at ${scope.school_name}` : " in select blocks"}.`
        : `- Aadhaar verification (${pct(overview.aadhaar_verified_pct)}) is ${overview.aadhaar_verified_pct >= 90 ? "above" : "at"} average, but ${lowAadhaarBlocks.length > 0 ? `${lowAadhaarBlocks.map(b => b.block_name).join(", ")} need attention` : "improvement needed"}.`,
      overview.completion_pct < avgCompletion 
        ? `- Data completion (${pct(overview.completion_pct)}) is below average (${pct(avgCompletion)}), indicating incomplete records${scope.block_name ? ` in ${scope.block_name}` : scope.school_name ? ` at ${scope.school_name}` : " in select blocks"}.`
        : `- Data completion (${pct(overview.completion_pct)}) is ${overview.completion_pct >= 95 ? "above" : "at"} average.`,
      overview.gender_parity_index < 0.8 
        ? `- Gender parity index (${overview.gender_parity_index}) is below target (0.8), indicating gender imbalance in teacher deployment.`
        : "",
      "",
      "## Recommendations",
      overview.ctet_pct < 70 
        ? `- Accelerate CTET training completion${scope.block_name ? ` in ${scope.block_name}` : scope.school_name ? ` at ${scope.school_name}` : ""}. Target: increase from ${pct(overview.ctet_pct)} to >70% within 2 months.`
        : `- Maintain CTET qualification rate above 70% to reach 100%.`,
      overview.aadhaar_verified_pct < 90 
        ? `- Run Aadhaar verification drives${scope.block_name ? ` in ${scope.block_name}` : scope.school_name ? ` at ${scope.school_name}` : ` targeting ${lowAadhaarBlocks.length > 0 ? lowAadhaarBlocks.map(b => b.block_name).join(", ") : "low-performing blocks"}`}. Target: increase from ${pct(overview.aadhaar_verified_pct)} to >90% within 1 month.`
        : `- Maintain Aadhaar verification above 90%. Focus on ${lowAadhaarBlocks.length > 0 ? lowAadhaarBlocks.map(b => b.block_name).join(", ") : "remaining schools"} to reach 100%.`,
      overview.completion_pct < 95 
        ? `- Complete data entry for missing records${scope.block_name ? ` in ${scope.block_name}` : scope.school_name ? ` at ${scope.school_name}` : ""}. Target: increase from ${pct(overview.completion_pct)} to >95% within 2 weeks.`
        : `- Maintain data completion above 95% to reach 100%.`,
      overview.nishtha_pct < 80 
        ? `- Accelerate NISHTHA training completion. Current rate: ${pct(overview.nishtha_pct)}, target: >80%.`
        : "",
      overview.gender_parity_index < 0.8 
        ? `- Improve gender balance in teacher deployment. Current GPI: ${overview.gender_parity_index}, target: >0.8.`
        : "",
      "",
      "## Priority Action Items",
      `- **Week 1**: ${overview.ctet_pct < 70 ? `Lift CTET qualification from ${pct(overview.ctet_pct)} to >70% by running training batches${scope.block_name ? ` in ${scope.block_name}` : scope.school_name ? ` at ${scope.school_name}` : ""}.` : `Maintain CTET qualification above 70%.`}`,
      `- **Week 2**: ${overview.aadhaar_verified_pct < 90 ? `Clear Aadhaar verification backlog (current ${pct(overview.aadhaar_verified_pct)}, target >90%)${scope.block_name ? ` in ${scope.block_name}` : scope.school_name ? ` at ${scope.school_name}` : lowAadhaarBlocks.length > 0 ? ` in ${lowAadhaarBlocks.slice(0, 2).map(b => b.block_name).join(", ")}` : ""}.` : `Maintain Aadhaar verification above 90%. Focus on ${lowAadhaarBlocks.length > 0 ? lowAadhaarBlocks.slice(0, 2).map(b => b.block_name).join(", ") : "remaining schools"}.`}`,
      `- **Week 3-4**: ${overview.completion_pct < 95 ? `Improve data completion to >95% (current ${pct(overview.completion_pct)})${scope.block_name ? ` in ${scope.block_name}` : scope.school_name ? ` at ${scope.school_name}` : ""}.` : `Maintain data completion above 95%.`}`,
      overview.nishtha_pct < 80 
        ? `- **Week 3-4**: Complete NISHTHA training for remaining teachers (current ${pct(overview.nishtha_pct)}, target >80%).`
        : "",
    ].filter(Boolean).join("\n");
  }, [overview, blockData, scope]);

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="loading-spinner" />
      </div>
    );
  }

  // Check if we have data - overview should exist and have meaningful data
  const hasData = overview && typeof overview === 'object' && (
    (overview.total_teachers > 0) || 
    (overview.total_schools > 0) ||
    (Object.keys(overview).length > 3)
  );
  
  // Debug log to help diagnose data loading issues
  if (overview) {
    console.log('Dashboard data check:', {
      hasData,
      totalTeachers: overview.total_teachers,
      totalSchools: overview.total_schools,
      overviewKeys: Object.keys(overview).length,
      overviewSample: Object.keys(overview).slice(0, 5)
    });
  }

  return (
    <div className="space-y-6 animate-fade-in" data-testid="ctteacher-dashboard">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight" style={{ fontFamily: 'Manrope' }}>
            CTTeacher Analytics Dashboard
          </h1>
          <p className="text-slate-500 mt-1">Teacher Data • 2025-26 • Pune District</p>
        </div>
        <div className="flex items-center gap-3">
          <Button 
            variant="outline" 
            size="sm" 
            onClick={handleImport}
            disabled={importing}
            data-testid="import-ctteacher-btn"
          >
            {importing ? <RefreshCw className="w-4 h-4 mr-2 animate-spin" /> : <Upload className="w-4 h-4 mr-2" />}
            {importing ? "Importing..." : "Import Data"}
          </Button>
          <ExportPanel dashboardName="ctteacher" dashboardTitle="CT Teacher Analytics" />
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
            <h3 className="text-xl font-semibold text-slate-700 mb-2">No Teacher Data Available</h3>
            <p className="text-slate-500 mb-4">Click "Import Data" to load the CTTeacher Excel file</p>
            <Button onClick={handleImport} disabled={importing} data-testid="import-ctteacher-empty-btn">
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
              <TabsTrigger value="profile" className="flex items-center gap-2" data-testid="tab-profile">
                <Users className="w-4 h-4" />
                Teacher Profile
              </TabsTrigger>
              <TabsTrigger value="deployment" className="flex items-center gap-2" data-testid="tab-deployment">
                <Target className="w-4 h-4" />
                Deployment & Risk
              </TabsTrigger>
              <TabsTrigger value="training" className="flex items-center gap-2" data-testid="tab-training">
                <GraduationCap className="w-4 h-4" />
                Training & Certification
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
                  label="Total Teachers"
                  value={overview.unique_teachers || overview.total_teachers}
                  icon={Users}
                  color="blue"
                  description={overview.unique_teachers && overview.unique_teachers !== overview.total_teachers 
                    ? `${overview.total_teachers?.toLocaleString()} records` 
                    : `${overview.total_schools?.toLocaleString()} schools`}
                />
                <KPICard
                  label="Schools Covered"
                  value={overview.total_schools}
                  icon={School}
                  color="purple"
                  description={`${overview.total_blocks} blocks`}
                />
                <KPICard
                  label="Aadhaar Verified"
                  value={overview.aadhaar_verified_pct}
                  suffix="%"
                  icon={ShieldCheck}
                  color={overview.aadhaar_verified_pct >= 90 ? "green" : "amber"}
                />
                <KPICard
                  label="Data Completion"
                  value={overview.completion_pct}
                  suffix="%"
                  icon={CheckCircle2}
                  color={overview.completion_pct >= 95 ? "green" : "amber"}
                />
                <KPICard
                  label="Avg/School"
                  value={overview.avg_teachers_per_school}
                  icon={Target}
                  color="cyan"
                />
              </div>

              {/* Compliance Gauges */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Compliance & Certification Metrics
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="flex flex-wrap justify-around gap-6 py-4">
                    <GaugeChart value={overview.aadhaar_verified_pct} label="Aadhaar Verified" />
                    <GaugeChart value={overview.completion_pct} label="Data Completion" />
                    <GaugeChart value={overview.ctet_pct} label="CTET Qualified" />
                    <GaugeChart value={overview.nishtha_pct} label="NISHTHA Completed" />
                  </div>
                </CardContent>
              </Card>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Gender Distribution */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Gender Distribution
                    </CardTitle>
                    <CardDescription>GPI: {overview.gender_parity_index}</CardDescription>
                  </CardHeader>
                  <CardContent>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={genderData}
                            cx="50%"
                            cy="50%"
                            innerRadius={50}
                            outerRadius={80}
                            paddingAngle={2}
                            dataKey="count"
                            label={({ gender, count }) => `${gender}: ${count.toLocaleString()}`}
                          >
                            {genderData.map((entry, index) => (
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

                {/* Block-wise Distribution */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Teachers by Block (Top 10)
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-64">
                      <ResponsiveContainer width="100%" height="100%">
                        <BarChart data={blockData.slice(0, 10)} layout="vertical" margin={{ left: 80 }}>
                          <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                          <XAxis type="number" />
                          <YAxis dataKey="block_name" type="category" tick={{ fontSize: 11 }} />
                          <Tooltip content={<CustomTooltip />} />
                          <Bar dataKey="total_teachers" name="Teachers" fill="#3b82f6" radius={[0, 4, 4, 0]} />
                        </BarChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>
              </div>

              {/* Block-wise Table */}
              <Card className="border-slate-200">
                <CardHeader className="pb-2">
                  <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                    Block-wise Teacher Distribution
                  </CardTitle>
                </CardHeader>
                <CardContent>
                  <div className="overflow-x-auto">
                    <Table>
                      <TableHeader>
                        <TableRow className="bg-slate-50">
                          <TableHead>Rank</TableHead>
                          <TableHead>Block</TableHead>
                          <TableHead className="text-right">Teachers</TableHead>
                          <TableHead className="text-right">Schools</TableHead>
                          <TableHead className="text-right">Avg/School</TableHead>
                          <TableHead className="text-right">Female %</TableHead>
                          <TableHead className="text-right">GPI</TableHead>
                          <TableHead className="text-right">Aadhaar %</TableHead>
                        </TableRow>
                      </TableHeader>
                      <TableBody>
                        {blockData.map((block) => (
                          <TableRow key={block.block_name}>
                            <TableCell className="text-slate-500">{block.rank}</TableCell>
                            <TableCell className="font-medium">
                              <BlockLink blockCode={block.block_code}>{block.block_name}</BlockLink>
                            </TableCell>
                            <TableCell className="text-right tabular-nums">{block.total_teachers?.toLocaleString()}</TableCell>
                            <TableCell className="text-right tabular-nums">{block.total_schools}</TableCell>
                            <TableCell className="text-right tabular-nums">{block.avg_per_school}</TableCell>
                            <TableCell className="text-right">
                              <Badge className="bg-pink-100 text-pink-700">{block.female_pct}%</Badge>
                            </TableCell>
                            <TableCell className="text-right tabular-nums">{block.gpi}</TableCell>
                            <TableCell className="text-right">
                              <Badge className={block.aadhaar_pct >= 90 ? "bg-emerald-100 text-emerald-700" : "bg-amber-100 text-amber-700"}>
                                {block.aadhaar_pct}%
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

            {/* DASHBOARD 2: TEACHER PROFILE & EQUITY */}
            <TabsContent value="profile" className="space-y-6">
              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                <KPICard label="Male Teachers" value={overview.male_count} icon={Users} color="blue" />
                <KPICard label="Female Teachers" value={overview.female_count} icon={Users} color="pink" description={`${overview.female_pct}% of total`} />
                <KPICard label="Gender Parity Index" value={overview.gender_parity_index} icon={Scale} color={overview.gender_parity_index >= 0.8 ? "green" : "amber"} />
                {ageData && <KPICard label="Average Age" value={ageData.avg_age} suffix="yrs" icon={Clock} color="purple" />}
              </div>

              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Social Category */}
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Social Category Distribution
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="h-72">
                      <ResponsiveContainer width="100%" height="100%">
                        <PieChart>
                          <Pie
                            data={socialCategory}
                            cx="50%"
                            cy="50%"
                            innerRadius={50}
                            outerRadius={90}
                            paddingAngle={2}
                            dataKey="count"
                          >
                            {socialCategory.map((entry, index) => (
                              <Cell key={`cell-${index}`} fill={entry.color} />
                            ))}
                          </Pie>
                          <Tooltip content={<CustomTooltip />} />
                          <Legend formatter={(value, entry) => entry.payload.category} />
                        </PieChart>
                      </ResponsiveContainer>
                    </div>
                  </CardContent>
                </Card>

                {/* Age Distribution */}
                {ageData && (
                  <Card className="border-slate-200">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                        Age Distribution
                      </CardTitle>
                      <CardDescription>Median: {ageData.median_age} yrs | Ageing Risk: {ageData.ageing_risk_pct}%</CardDescription>
                    </CardHeader>
                    <CardContent>
                      <div className="h-72">
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart data={ageData.distribution}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                            <XAxis dataKey="age_band" />
                            <YAxis />
                            <Tooltip content={<CustomTooltip />} />
                            <Bar dataKey="count" name="Teachers" fill="#8b5cf6" radius={[4, 4, 0, 0]} />
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                    </CardContent>
                  </Card>
                )}
              </div>

              {/* Qualification */}
              {qualification && (
                <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                  <Card className="border-slate-200">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                        Academic Qualification
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="h-64">
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart data={qualification.academic} layout="vertical" margin={{ left: 100 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                            <XAxis type="number" />
                            <YAxis dataKey="qualification" type="category" tick={{ fontSize: 11 }} />
                            <Tooltip content={<CustomTooltip />} />
                            <Bar dataKey="count" name="Teachers" fill="#3b82f6" radius={[0, 4, 4, 0]} />
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                    </CardContent>
                  </Card>

                  <Card className="border-slate-200">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                        Professional Qualification
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="h-64">
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart data={qualification.professional.slice(0, 8)} layout="vertical" margin={{ left: 80 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                            <XAxis type="number" />
                            <YAxis dataKey="qualification" type="category" tick={{ fontSize: 11 }} />
                            <Tooltip content={<CustomTooltip />} />
                            <Bar dataKey="count" name="Teachers" fill="#10b981" radius={[0, 4, 4, 0]} />
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                    </CardContent>
                  </Card>
                </div>
              )}
            </TabsContent>

            {/* DASHBOARD 3: DEPLOYMENT & RISK */}
            <TabsContent value="deployment" className="space-y-6">
              {tenureData && (
                <>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <KPICard label="Avg Service Years" value={tenureData.avg_tenure} suffix="yrs" icon={Clock} color="blue" />
                    <KPICard label="New Entrants" value={tenureData.new_entrant_pct} suffix="%" icon={Users} color="green" description="≤5 years service" />
                    <KPICard label="Retirement Risk" value={tenureData.retirement_risk_pct} suffix="%" icon={AlertTriangle} color={tenureData.retirement_risk_pct > 10 ? "red" : "amber"} description=">25 years service" />
                    {ageData && <KPICard label="Ageing Risk" value={ageData.ageing_risk_pct} suffix="%" icon={AlertTriangle} color={ageData.ageing_risk_pct > 20 ? "red" : "amber"} description=">55 years age" />}
                  </div>

                  <Card className="border-slate-200">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                        Service Tenure Distribution
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="h-72">
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart data={tenureData.distribution}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                            <XAxis dataKey="tenure_band" />
                            <YAxis />
                            <Tooltip content={<CustomTooltip />} />
                            <Bar dataKey="count" name="Teachers" radius={[4, 4, 0, 0]}>
                              {tenureData.distribution.map((entry, index) => (
                                <Cell key={`cell-${index}`} fill={index < 2 ? "#10b981" : (index > 3 ? "#ef4444" : "#f59e0b")} />
                              ))}
                            </Bar>
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                    </CardContent>
                  </Card>
                </>
              )}

              {/* Data Quality */}
              {dataQuality && (
                <Card className="border-slate-200">
                  <CardHeader className="pb-2">
                    <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                      Data Quality & Compliance Metrics
                    </CardTitle>
                  </CardHeader>
                  <CardContent>
                    <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                      <div className="p-4 bg-blue-50 rounded-lg text-center">
                        <p className="text-xs text-slate-500">Data Quality Score</p>
                        <p className="text-2xl font-bold text-blue-700">{dataQuality.data_quality_score}%</p>
                      </div>
                      <div className="p-4 bg-emerald-50 rounded-lg text-center">
                        <p className="text-xs text-slate-500">Total Records</p>
                        <p className="text-2xl font-bold text-emerald-700">{dataQuality.total_records?.toLocaleString()}</p>
                      </div>
                      <div className="p-4 bg-amber-50 rounded-lg text-center">
                        <p className="text-xs text-slate-500">Aadhaar Issues</p>
                        <p className="text-2xl font-bold text-amber-700">{dataQuality.aadhaar_issues_pct}%</p>
                      </div>
                      <div className="p-4 bg-red-50 rounded-lg text-center">
                        <p className="text-xs text-slate-500">Not Completed</p>
                        <p className="text-2xl font-bold text-red-700">{dataQuality.not_completed_pct}%</p>
                      </div>
                      <div className="p-4 bg-purple-50 rounded-lg text-center">
                        <p className="text-xs text-slate-500">Missing CRR</p>
                        <p className="text-2xl font-bold text-purple-700">{dataQuality.missing_crr_pct}%</p>
                      </div>
                    </div>
                  </CardContent>
                </Card>
              )}
            </TabsContent>

            {/* DASHBOARD 4: TRAINING & CERTIFICATION */}
            <TabsContent value="training" className="space-y-6">
              {certification && (
                <>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                    <KPICard label="CTET Qualified" value={certification.ctet?.qualified} icon={Award} color="green" description={`${certification.ctet?.qualified_pct}% of total`} />
                    <KPICard label="CTET Unknown" value={certification.ctet?.unknown} icon={AlertTriangle} color="amber" />
                    <KPICard label="NISHTHA Completed" value={certification.nishtha?.completed} icon={GraduationCap} color="blue" description={`${certification.nishtha?.completed_pct}% of total`} />
                    <KPICard label="Training Demand" value={trainingData.length} suffix="types" icon={Target} color="purple" />
                  </div>

                  <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                    {/* CTET Status */}
                    <Card className="border-slate-200">
                      <CardHeader className="pb-2">
                        <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                          CTET Qualification Status
                        </CardTitle>
                      </CardHeader>
                      <CardContent>
                        <div className="h-64">
                          <ResponsiveContainer width="100%" height="100%">
                            <PieChart>
                              <Pie
                                data={[
                                  { name: "Qualified", value: certification.ctet?.qualified, color: "#10b981" },
                                  { name: "Not Qualified", value: certification.ctet?.not_qualified, color: "#ef4444" },
                                  { name: "Unknown", value: certification.ctet?.unknown, color: "#f59e0b" }
                                ]}
                                cx="50%"
                                cy="50%"
                                innerRadius={50}
                                outerRadius={80}
                                paddingAngle={2}
                                dataKey="value"
                              >
                                {[
                                  { color: "#10b981" },
                                  { color: "#ef4444" },
                                  { color: "#f59e0b" }
                                ].map((entry, index) => (
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

                    {/* NISHTHA Status */}
                    <Card className="border-slate-200">
                      <CardHeader className="pb-2">
                        <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                          NISHTHA Training Status
                        </CardTitle>
                      </CardHeader>
                      <CardContent>
                        <div className="h-64">
                          <ResponsiveContainer width="100%" height="100%">
                            <PieChart>
                              <Pie
                                data={[
                                  { name: "Completed", value: certification.nishtha?.completed, color: "#3b82f6" },
                                  { name: "Not Completed", value: certification.nishtha?.not_completed, color: "#94a3b8" }
                                ]}
                                cx="50%"
                                cy="50%"
                                innerRadius={50}
                                outerRadius={80}
                                paddingAngle={2}
                                dataKey="value"
                              >
                                {[
                                  { color: "#3b82f6" },
                                  { color: "#94a3b8" }
                                ].map((entry, index) => (
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

                  {/* Training Demand */}
                  <Card className="border-slate-200">
                    <CardHeader className="pb-2">
                      <CardTitle className="text-lg" style={{ fontFamily: 'Manrope' }}>
                        Training Demand Analysis
                      </CardTitle>
                    </CardHeader>
                    <CardContent>
                      <div className="h-72">
                        <ResponsiveContainer width="100%" height="100%">
                          <BarChart data={trainingData.slice(0, 10)} layout="vertical" margin={{ left: 120 }}>
                            <CartesianGrid strokeDasharray="3 3" stroke="#e2e8f0" />
                            <XAxis type="number" />
                            <YAxis dataKey="training_type" type="category" tick={{ fontSize: 10 }} width={110} />
                            <Tooltip content={<CustomTooltip />} />
                            <Bar dataKey="count" name="Teachers" fill="#8b5cf6" radius={[0, 4, 4, 0]} />
                          </BarChart>
                        </ResponsiveContainer>
                      </div>
                    </CardContent>
                  </Card>
                </>
              )}
            </TabsContent>
            <TabsContent value="insights" className="space-y-6">
              <AiInsightsTab
                title="CTTeacher Analytics Insights"
                description="AI insights, root cause signals, recommendations, and priority actions for CTTeacher analytics."
                generate={buildInsights}
                autoGenerate={true}
                dataDependency={overview}
              />
            </TabsContent>
          </Tabs>
        </>
      )}
    </div>
  );
};

export default CTTeacherDashboard;
