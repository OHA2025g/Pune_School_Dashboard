import { useState, useEffect } from "react";
import { useNavigate } from "react-router-dom";
import axios from "axios";
import KPICard from "../components/KPICard";
import DistrictCard from "../components/DistrictCard";
import DataTable from "../components/DataTable";
import RAGBadge from "../components/RAGBadge";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { 
  School, 
  Users, 
  ShieldCheck, 
  Droplets, 
  GraduationCap,
  FileCheck,
  Trophy,
  TrendingUp,
  TrendingDown,
  RefreshCw,
  MapPin,
  ArrowUpRight
} from "lucide-react";
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
  Pie
} from "recharts";

import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;

const StateOverview = () => {
  const navigate = useNavigate();
  const [loading, setLoading] = useState(true);
  const [kpiData, setKpiData] = useState(null);
  const [districts, setDistricts] = useState([]);
  const [view, setView] = useState("grid");
  const [sortBy, setSortBy] = useState("shi_score");
  const [sortOrder, setSortOrder] = useState("desc");

  const fetchData = async () => {
    setLoading(true);
    try {
      const [kpiRes, districtsRes] = await Promise.all([
        axios.get(`${API}/state/overview`),
        axios.get(`${API}/districts?sort_by=${sortBy}&sort_order=${sortOrder}`)
      ]);
      setKpiData(kpiRes.data);
      setDistricts(districtsRes.data);
    } catch (error) {
      console.error("Error fetching data:", error);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchData();
  }, [sortBy, sortOrder]);

  const handleSort = (key, order) => {
    setSortBy(key);
    setSortOrder(order);
  };

  const tableColumns = [
    { key: "district_name", label: "District", sortable: true },
    { key: "total_schools", label: "Schools", type: "number", sortable: true },
    { key: "total_students", label: "Students", type: "number", sortable: true },
    { key: "aadhaar_percentage", label: "Aadhaar %", type: "percentage", sortable: true },
    { key: "apaar_percentage", label: "APAAR %", type: "percentage", sortable: true },
    { key: "avg_ptr", label: "Avg PTR", type: "number", sortable: true },
    { key: "shi_score", label: "SHI Score", type: "progress", sortable: true },
    { key: "rag_status", label: "Status", type: "rag" },
    { key: "action", label: "", type: "action" },
  ];

  const getRAGDistribution = () => {
    const green = districts.filter(d => d.rag_status === "green").length;
    const amber = districts.filter(d => d.rag_status === "amber").length;
    const red = districts.filter(d => d.rag_status === "red").length;
    return [
      { name: "Excellent", value: green, color: "#10b981" },
      { name: "At Risk", value: amber, color: "#f59e0b" },
      { name: "Critical", value: red, color: "#ef4444" },
    ];
  };

  const getTopDistricts = () => {
    return [...districts]
      .sort((a, b) => b.shi_score - a.shi_score)
      .slice(0, 5);
  };

  const getBottomDistricts = () => {
    return [...districts]
      .sort((a, b) => a.shi_score - b.shi_score)
      .slice(0, 5);
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="loading-spinner" />
      </div>
    );
  }

  return (
    <div className="space-y-6 animate-fade-in" data-testid="state-overview">
      {/* Header */}
      <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight" style={{ fontFamily: 'Manrope' }}>
            Maharashtra Education Dashboard
          </h1>
          <p className="text-slate-500 mt-1">Academic Year 2025-26 • Real-time Overview</p>
        </div>
        <div className="flex items-center gap-3">
          <Button variant="outline" size="sm" onClick={fetchData} data-testid="refresh-btn">
            <RefreshCw className="w-4 h-4 mr-2" />
            Refresh
          </Button>
        </div>
      </div>

      {/* KPI Cards */}
      <div className="kpi-grid">
        <KPICard
          label="Total Schools"
          value={kpiData?.total_schools}
          icon={School}
          trend="up"
          trendValue="+2.3% from last year"
          testId="kpi-total-schools"
        />
        <KPICard
          label="Total Students"
          value={kpiData?.total_students}
          icon={Users}
          trend="up"
          trendValue="+1.8% from last year"
          testId="kpi-total-students"
        />
        <KPICard
          label="Aadhaar Compliance"
          value={kpiData?.aadhaar_percentage}
          suffix="%"
          icon={ShieldCheck}
          trend="up"
          trendValue="+5.2% this month"
          testId="kpi-aadhaar"
        />
        <KPICard
          label="APAAR Coverage"
          value={kpiData?.apaar_percentage}
          suffix="%"
          icon={FileCheck}
          trend="up"
          trendValue="+8.1% this month"
          testId="kpi-apaar"
        />
        <KPICard
          label="Water Availability"
          value={kpiData?.water_availability_percentage}
          suffix="%"
          icon={Droplets}
          trend="stable"
          trendValue="No change"
          testId="kpi-water"
        />
        <KPICard
          label="Average PTR"
          value={kpiData?.avg_ptr}
          icon={GraduationCap}
          trend="down"
          trendValue="-2.1 improved"
          testId="kpi-ptr"
        />
        <KPICard
          label="Data Entry Complete"
          value={kpiData?.data_entry_percentage}
          suffix="%"
          icon={FileCheck}
          trend="up"
          trendValue="+12.4% this week"
          testId="kpi-data-entry"
        />
        <KPICard
          label="Avg SHI Score"
          value={kpiData?.avg_shi}
          icon={Trophy}
          trend="up"
          trendValue="+3.2 this quarter"
          testId="kpi-shi"
        />
      </div>

      {/* Charts Row */}
      <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
        {/* RAG Distribution */}
        <Card className="border-slate-200">
          <CardHeader className="pb-2">
            <CardTitle className="text-lg font-semibold" style={{ fontFamily: 'Manrope' }}>
              District Health Distribution
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="h-64">
              <ResponsiveContainer width="100%" height="100%">
                <PieChart>
                  <Pie
                    data={getRAGDistribution()}
                    cx="50%"
                    cy="50%"
                    innerRadius={60}
                    outerRadius={90}
                    paddingAngle={2}
                    dataKey="value"
                  >
                    {getRAGDistribution().map((entry, index) => (
                      <Cell key={`cell-${index}`} fill={entry.color} />
                    ))}
                  </Pie>
                  <Tooltip 
                    contentStyle={{ 
                      backgroundColor: 'rgba(15, 23, 42, 0.95)',
                      border: 'none',
                      borderRadius: '8px',
                      color: 'white'
                    }}
                  />
                </PieChart>
              </ResponsiveContainer>
            </div>
            <div className="flex justify-center gap-6 mt-4">
              {getRAGDistribution().map((item) => (
                <div key={item.name} className="flex items-center gap-2">
                  <div 
                    className="w-3 h-3 rounded-full" 
                    style={{ backgroundColor: item.color }}
                  />
                  <span className="text-sm text-slate-600">{item.name}: {item.value}</span>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        {/* Top Districts */}
        <Card className="border-slate-200">
          <CardHeader className="pb-2">
            <CardTitle className="text-lg font-semibold flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
              <TrendingUp className="w-5 h-5 text-emerald-500" />
              Top Performing Districts
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {getTopDistricts().map((district, idx) => (
                <div 
                  key={district.district_code}
                  className="flex items-center justify-between p-3 bg-slate-50 rounded-lg cursor-pointer hover:bg-slate-100 transition-colors"
                  onClick={() => navigate(`/district/${district.district_code}`)}
                  data-testid={`top-district-${idx}`}
                >
                  <div className="flex items-center gap-3">
                    <span className="w-6 h-6 flex items-center justify-center bg-emerald-100 text-emerald-700 text-xs font-bold rounded-full">
                      {idx + 1}
                    </span>
                    <div>
                      <p className="font-medium text-slate-900">{district.district_name}</p>
                      <p className="text-xs text-slate-500">{district.total_schools} schools</p>
                    </div>
                  </div>
                  <div className="text-right">
                    <p className="font-bold text-emerald-600 tabular-nums">{district.shi_score}</p>
                    <p className="text-xs text-slate-500">SHI Score</p>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>

        {/* Bottom Districts */}
        <Card className="border-slate-200">
          <CardHeader className="pb-2">
            <CardTitle className="text-lg font-semibold flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
              <TrendingDown className="w-5 h-5 text-red-500" />
              Districts Needing Attention
            </CardTitle>
          </CardHeader>
          <CardContent>
            <div className="space-y-3">
              {getBottomDistricts().map((district, idx) => (
                <div 
                  key={district.district_code}
                  className="flex items-center justify-between p-3 bg-slate-50 rounded-lg cursor-pointer hover:bg-slate-100 transition-colors"
                  onClick={() => navigate(`/district/${district.district_code}`)}
                  data-testid={`bottom-district-${idx}`}
                >
                  <div className="flex items-center gap-3">
                    <span className="w-6 h-6 flex items-center justify-center bg-red-100 text-red-700 text-xs font-bold rounded-full">
                      {idx + 1}
                    </span>
                    <div>
                      <p className="font-medium text-slate-900">{district.district_name}</p>
                      <p className="text-xs text-slate-500">{district.total_schools} schools</p>
                    </div>
                  </div>
                  <div className="text-right">
                    <p className="font-bold text-red-600 tabular-nums">{district.shi_score}</p>
                    <p className="text-xs text-slate-500">SHI Score</p>
                  </div>
                </div>
              ))}
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Districts Section */}
      <Card className="border-slate-200">
        <CardHeader className="pb-4">
          <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
            <CardTitle className="text-xl font-semibold" style={{ fontFamily: 'Manrope' }}>
              All Districts ({districts.length})
            </CardTitle>
            <div className="flex items-center gap-3">
              <Select value={sortBy} onValueChange={(value) => handleSort(value, sortOrder)}>
                <SelectTrigger className="w-40" data-testid="sort-select">
                  <SelectValue placeholder="Sort by" />
                </SelectTrigger>
                <SelectContent>
                  <SelectItem value="shi_score">SHI Score</SelectItem>
                  <SelectItem value="district_name">Name</SelectItem>
                  <SelectItem value="total_schools">Schools</SelectItem>
                  <SelectItem value="total_students">Students</SelectItem>
                  <SelectItem value="aadhaar_percentage">Aadhaar %</SelectItem>
                </SelectContent>
              </Select>
              
              <Tabs value={view} onValueChange={setView}>
                <TabsList>
                  <TabsTrigger value="grid" data-testid="view-grid">Grid</TabsTrigger>
                  <TabsTrigger value="table" data-testid="view-table">Table</TabsTrigger>
                </TabsList>
              </Tabs>
            </div>
          </div>
        </CardHeader>
        <CardContent>
          {view === "grid" ? (
            <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 xl:grid-cols-4 gap-4">
              {districts.map((district) => (
                <DistrictCard key={district.district_code} district={district} />
              ))}
            </div>
          ) : (
            <DataTable 
              data={districts}
              columns={tableColumns}
              sortBy={sortBy}
              sortOrder={sortOrder}
              onSort={handleSort}
              onRowClick={(row) => navigate(`/district/${row.district_code}`)}
              testId="districts-table"
            />
          )}
        </CardContent>
      </Card>
    </div>
  );
};

export default StateOverview;
