import { useState, useEffect } from "react";
import { useParams, useNavigate } from "react-router-dom";
import axios from "axios";
import RAGBadge from "../components/RAGBadge";
import ProgressBar from "../components/ProgressBar";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Separator } from "@/components/ui/separator";
import { 
  ArrowLeft,
  School, 
  Users, 
  GraduationCap,
  Droplets,
  Building2,
  MapPin,
  ShieldCheck,
  FileCheck,
  Trophy,
  RefreshCw,
  CheckCircle2,
  XCircle,
  Brain,
  Loader2
} from "lucide-react";
import { toast } from "sonner";
import AiInsightsCard from "@/components/AiInsightsCard";

import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;

const SchoolDetail = () => {
  const { udiseCode } = useParams();
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [school, setSchool] = useState(null);
  const [insightsLoading, setInsightsLoading] = useState(false);
  const [insights, setInsights] = useState("");

  const fetchData = async () => {
    setLoading(true);
    try {
      const response = await axios.get(`${API}/schools/${udiseCode}`);
      setSchool(response.data);
    } catch (error) {
      console.error("Error fetching school data:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [udiseCode]);

  const generateInsights = async () => {
    setInsightsLoading(true);
    try {
      const res = await axios.get(`${API}/analytics/insights/executive-summary`, {
        params: { udise_code: udiseCode },
      });
      setInsights(res.data?.ai_summary || "");
      toast.success("Insights generated");
    } catch (error) {
      const status = error?.response?.status;
      const detail = error?.response?.data?.detail || error?.message || "Failed to generate insights";
      if (status === 401) navigate("/login");
      toast.error(detail);
    } finally {
      setInsightsLoading(false);
    }
  };

  const refreshAll = async () => {
    await fetchData();
    if (insights) await generateInsights();
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="loading-spinner" />
      </div>
    );
  }

  if (!school) {
    return (
      <div className="text-center py-12">
        <p className="text-slate-500">School not found</p>
        <Button variant="outline" onClick={() => navigate(-1)} className="mt-4">
          Go Back
        </Button>
      </div>
    );
  }

  const InfoItem = ({ icon: Icon, label, value, status }) => (
    <div className="flex items-start gap-3 p-4 bg-slate-50 rounded-lg">
      <div className="p-2 bg-white rounded-lg shadow-sm">
        <Icon className="w-5 h-5 text-slate-600" strokeWidth={1.5} />
      </div>
      <div className="flex-1">
        <p className="text-xs font-medium text-slate-500 uppercase tracking-wider">{label}</p>
        <p className="text-lg font-semibold text-slate-900 mt-1">
          {typeof value === 'boolean' ? (
            value ? (
              <span className="flex items-center gap-1 text-emerald-600">
                <CheckCircle2 className="w-5 h-5" /> Available
              </span>
            ) : (
              <span className="flex items-center gap-1 text-red-600">
                <XCircle className="w-5 h-5" /> Not Available
              </span>
            )
          ) : (
            value
          )}
        </p>
      </div>
    </div>
  );

  return (
    <div className="space-y-6 animate-fade-in" data-testid="school-detail">
      {/* Header */}
      <div className="flex items-start justify-between">
        <div className="flex items-start gap-4">
          <Button 
            variant="ghost" 
            size="icon" 
            onClick={() => navigate(`/block/${school.block_code}`)}
            data-testid="back-btn"
          >
            <ArrowLeft className="w-5 h-5" />
          </Button>
          <div>
            <div className="flex items-center gap-3 mb-2">
              <h1 className="text-2xl font-bold text-slate-900 tracking-tight" style={{ fontFamily: 'Manrope' }}>
                {school.school_name}
              </h1>
              <RAGBadge status={school.rag_status} size="large" />
            </div>
            <div className="flex flex-wrap items-center gap-4 text-slate-500 text-sm">
              <span className="flex items-center gap-1">
                <MapPin className="w-4 h-4" />
                {school.district_name} • {school.block_name}
              </span>
              <span className="flex items-center gap-1">
                <School className="w-4 h-4" />
                UDISE: {school.udise_code}
              </span>
              <span className="px-2 py-0.5 bg-slate-100 rounded text-xs font-medium">
                {school.school_category}
              </span>
              <span className="px-2 py-0.5 bg-blue-100 text-blue-700 rounded text-xs font-medium">
                {school.school_management}
              </span>
            </div>
          </div>
        </div>
        <Button variant="outline" size="sm" onClick={refreshAll}>
          <RefreshCw className="w-4 h-4 mr-2" />
          Refresh
        </Button>
      </div>

      {/* SHI Score Card */}
      <Card className="border-slate-200 bg-gradient-to-br from-slate-900 to-slate-800 text-white">
        <CardContent className="p-6">
          <div className="flex items-center justify-between">
            <div>
              <p className="text-slate-400 text-sm font-medium uppercase tracking-wider">School Health Index</p>
              <div className="flex items-baseline gap-2 mt-2">
                <span className="text-5xl font-bold tabular-nums" style={{ fontFamily: 'Manrope' }}>
                  {school.shi_score}
                </span>
                <span className="text-2xl text-slate-400">/100</span>
              </div>
              <p className="text-slate-400 mt-2">
                {school.shi_score >= 85 ? "Excellent Performance" :
                 school.shi_score >= 70 ? "Good Performance" :
                 school.shi_score >= 50 ? "Needs Improvement" : "Critical - Immediate Action Required"}
              </p>
            </div>
            <div className="p-4 bg-white/10 rounded-xl">
              <Trophy className="w-12 h-12 text-amber-400" />
            </div>
          </div>
          <div className="mt-6">
            <div className="h-3 bg-slate-700 rounded-full overflow-hidden">
              <div 
                className={`h-full rounded-full transition-all duration-500 ${
                  school.shi_score >= 85 ? 'bg-emerald-500' :
                  school.shi_score >= 70 ? 'bg-amber-500' : 'bg-red-500'
                }`}
                style={{ width: `${school.shi_score}%` }}
              />
            </div>
          </div>
        </CardContent>
      </Card>

      {/* AI Insights */}
      {!insights ? (
        <Card className="border-slate-200">
          <CardHeader>
            <CardTitle style={{ fontFamily: "Manrope" }}>AI Insights (School)</CardTitle>
          </CardHeader>
          <CardContent className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
            <div className="text-slate-600">
              Generate scoped insights for this school: Insights, Root Cause Signals, Recommendations, Priority Action Items.
            </div>
            <Button onClick={generateInsights} disabled={insightsLoading}>
              {insightsLoading ? (
                <>
                  <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                  Generating…
                </>
              ) : (
                <>
                  <Brain className="w-4 h-4 mr-2" />
                  Generate Insights
                </>
              )}
            </Button>
          </CardContent>
        </Card>
      ) : (
        <AiInsightsCard title="AI Insights (School)" content={insights} loading={insightsLoading} />
      )}

      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* Student & Staff Info */}
        <Card className="border-slate-200">
          <CardHeader>
            <CardTitle className="flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
              <Users className="w-5 h-5 text-blue-600" />
              Students & Staff
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <InfoItem 
              icon={Users} 
              label="Total Students" 
              value={school.total_students?.toLocaleString('en-IN')} 
            />
            <InfoItem 
              icon={GraduationCap} 
              label="Total Teachers" 
              value={school.total_teachers?.toLocaleString('en-IN')} 
            />
            <InfoItem 
              icon={GraduationCap} 
              label="Pupil-Teacher Ratio" 
              value={`${school.ptr} : 1`} 
            />
          </CardContent>
        </Card>

        {/* Identity Compliance */}
        <Card className="border-slate-200">
          <CardHeader>
            <CardTitle className="flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
              <ShieldCheck className="w-5 h-5 text-emerald-600" />
              Identity Compliance
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <div className="p-4 bg-slate-50 rounded-lg">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm font-medium text-slate-600">Aadhaar Compliance</span>
                <span className="font-bold text-slate-900">{school.aadhaar_percentage}%</span>
              </div>
              <ProgressBar value={school.aadhaar_percentage} showLabel={false} />
            </div>
            <div className="p-4 bg-slate-50 rounded-lg">
              <div className="flex items-center justify-between mb-2">
                <span className="text-sm font-medium text-slate-600">APAAR Coverage</span>
                <span className="font-bold text-slate-900">{school.apaar_percentage}%</span>
              </div>
              <ProgressBar value={school.apaar_percentage} showLabel={false} />
            </div>
          </CardContent>
        </Card>

        {/* Infrastructure */}
        <Card className="border-slate-200">
          <CardHeader>
            <CardTitle className="flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
              <Building2 className="w-5 h-5 text-purple-600" />
              Infrastructure
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <InfoItem 
              icon={Droplets} 
              label="Drinking Water" 
              value={school.water_available} 
            />
            <InfoItem 
              icon={Building2} 
              label="Toilets Available" 
              value={school.toilets_available} 
            />
            <InfoItem 
              icon={School} 
              label="Classrooms" 
              value={school.classrooms} 
            />
            <InfoItem 
              icon={Users} 
              label="Students per Classroom" 
              value={school.students_per_classroom?.toFixed(1)} 
            />
          </CardContent>
        </Card>

        {/* Data Quality */}
        <Card className="border-slate-200">
          <CardHeader>
            <CardTitle className="flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
              <FileCheck className="w-5 h-5 text-amber-600" />
              Data Quality
            </CardTitle>
          </CardHeader>
          <CardContent className="space-y-4">
            <InfoItem 
              icon={FileCheck} 
              label="Data Entry Status" 
              value={school.data_entry_status === "completed" ? "Completed" : "Pending"} 
            />
            <InfoItem 
              icon={CheckCircle2} 
              label="Certified" 
              value={school.certified} 
            />
          </CardContent>
        </Card>
      </div>
    </div>
  );
};

export default SchoolDetail;
