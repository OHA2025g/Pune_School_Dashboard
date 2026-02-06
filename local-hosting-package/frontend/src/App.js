import "@/App.css";
import { useState, useEffect } from "react";
import { BrowserRouter, HashRouter, Routes, Route, Navigate } from "react-router-dom";
import axios from "axios";
import DashboardLayout from "./components/DashboardLayout";
import StateOverview from "./pages/StateOverview";
import SchoolHealthIndex from "./pages/SchoolHealthIndex";
import DistrictDetail from "./pages/DistrictDetail";
import BlockDetail from "./pages/BlockDetail";
import SchoolDetail from "./pages/SchoolDetail";
import DataImport from "./pages/DataImport";
import AadhaarDashboard from "./pages/AadhaarDashboard";
import TeacherDashboard from "./pages/TeacherDashboard";
import InfrastructureDashboard from "./pages/InfrastructureDashboard";
import EnrolmentDashboard from "./pages/EnrolmentDashboard";
import DropboxDashboard from "./pages/DropboxDashboard";
import DataEntryDashboard from "./pages/DataEntryDashboard";
import AgeEnrolmentDashboard from "./pages/AgeEnrolmentDashboard";
import CTTeacherDashboard from "./pages/CTTeacherDashboard";
import APAARDashboard from "./pages/APAARDashboard";
import ClassroomsToiletsDashboard from "./pages/ClassroomsToiletsDashboard";
import ExecutiveDashboard from "./pages/ExecutiveDashboard";
import AnalyticsDashboard from "./pages/AnalyticsDashboard";
import LoginPage from "./pages/LoginPage";
import UserManagement from "./pages/UserManagement";
import { Toaster } from "@/components/ui/sonner";
import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();
const BACKEND_ORIGIN = (() => {
  try {
    return new URL(BACKEND_URL).origin;
  } catch {
    return BACKEND_URL;
  }
})();

// Avoid "infinite loading" if a request hangs (axios default timeout is 0 = no timeout).
// This is especially common when the frontend is opened via a LAN URL but the backend
// is still configured as localhost, or if a backend process is not reachable.
axios.defaults.timeout = 20000; // 20s

const IS_GITHUB_PAGES = typeof window !== "undefined" && window.location.hostname.endsWith("github.io");
const ROUTER_BASENAME = process.env.PUBLIC_URL || "/";

// Protected Route component
const ProtectedRoute = ({ children, user, allowedRoles }) => {
  if (!user) {
    return <Navigate to="/login" replace />;
  }
  
  if (allowedRoles && !allowedRoles.includes(user.role)) {
    return <Navigate to="/executive-dashboard" replace />;
  }
  
  return children;
};

function App() {
  const [user, setUser] = useState(null);
  const [loading, setLoading] = useState(true);

  useEffect(() => {
    // Check for existing token on mount
    const token = localStorage.getItem("token");
    const savedUser = localStorage.getItem("user");
    
    if (token && savedUser) {
      try {
        const parsedUser = JSON.parse(savedUser);
        setUser(parsedUser);
        axios.defaults.headers.common["Authorization"] = `Bearer ${token}`;
      } catch (e) {
        localStorage.removeItem("token");
        localStorage.removeItem("user");
      }
    }
    setLoading(false);
  }, []);

  useEffect(() => {
    // Attach auth token (if present) + district/block/school scope (if set)
    // to all backend /api requests.
    const interceptorId = axios.interceptors.request.use((config) => {
      try {
        // Allow opt-out (used by ScopeFilter when fetching unscoped dropdown values)
        if (config?.headers && config.headers["x-skip-scope"] === "true") return config;

        const url = typeof config.url === "string" ? config.url : "";
        const fullUrl = new URL(url, window.location.origin);
        if (fullUrl.origin !== BACKEND_ORIGIN) return config;
        if (!fullUrl.pathname.startsWith("/api/")) return config;

        // Ensure Authorization is present for protected endpoints (e.g. Advanced Analytics).
        // This is intentionally resilient to app refreshes / backend restarts where axios defaults
        // might not be set yet, but localStorage still has the token.
        const token = localStorage.getItem("token");
        if (token) {
          // Axios v1 may use AxiosHeaders which requires .set()/.get()
          const hasSet = typeof config?.headers?.set === "function";
          const hasGet = typeof config?.headers?.get === "function";

          if (hasSet) {
            const existing = hasGet ? config.headers.get("Authorization") : null;
            if (!existing) config.headers.set("Authorization", `Bearer ${token}`);
          } else {
            config.headers = config.headers || {};
            if (!config.headers.Authorization && !config.headers.authorization) {
              config.headers.Authorization = `Bearer ${token}`;
            }
          }
        }

        const raw = localStorage.getItem("dashboard_scope_v1");
        if (!raw) return config;
        const scope = JSON.parse(raw);
        if (!scope || typeof scope !== "object") return config;

        config.params = { ...(config.params || {}) };
        if (!config.params.district_code && scope.districtCode) config.params.district_code = scope.districtCode;
        if (!config.params.block_code && scope.blockCode) config.params.block_code = scope.blockCode;
        if (!config.params.udise_code && scope.udiseCode) config.params.udise_code = scope.udiseCode;
        if (!config.params.district_name && scope.districtName) config.params.district_name = scope.districtName;
        if (!config.params.block_name && scope.blockName) config.params.block_name = scope.blockName;
        if (!config.params.school_name && scope.schoolName) config.params.school_name = scope.schoolName;
      } catch {
        // ignore
      }
      return config;
    });

    return () => {
      axios.interceptors.request.eject(interceptorId);
    };
  }, []);

  const handleLogin = (userData) => {
    setUser(userData);
  };

  const handleLogout = () => {
    localStorage.removeItem("token");
    localStorage.removeItem("user");
    delete axios.defaults.headers.common["Authorization"];
    setUser(null);
  };

  if (loading) {
    return <div className="flex items-center justify-center h-screen"><div className="loading-spinner" /></div>;
  }

  return (
    <div className="App">
      {IS_GITHUB_PAGES && BACKEND_URL.includes("localhost") ? (
        <div className="mx-auto max-w-5xl px-4 pt-4">
          <div className="rounded-lg border border-amber-200 bg-amber-50 px-4 py-3 text-sm text-amber-900">
            <span className="font-semibold">Demo mode:</span>{" "}
            this GitHub Pages site hosts only the frontend UI. To see real data, run the backend + MongoDB locally and set{" "}
            <code className="px-1 py-0.5 rounded bg-amber-100">REACT_APP_BACKEND_URL</code>.
          </div>
        </div>
      ) : null}

      {IS_GITHUB_PAGES ? (
        <HashRouter>
          <Routes>
            {/* Public route */}
            <Route path="/login" element={
              user ? <Navigate to="/executive-dashboard" replace /> : <LoginPage onLogin={handleLogin} />
            } />
            
            {/* Protected routes */}
            <Route path="/" element={
              <ProtectedRoute user={user}>
                <DashboardLayout user={user} onLogout={handleLogout} />
              </ProtectedRoute>
            }>
              <Route index element={<StateOverview />} />
              <Route path="health-index" element={<SchoolHealthIndex />} />
              <Route path="data-import" element={
                <ProtectedRoute user={user} allowedRoles={["admin"]}>
                  <DataImport />
                </ProtectedRoute>
              } />
              <Route path="aadhaar-analytics" element={<AadhaarDashboard />} />
              <Route path="teacher-dashboard" element={<TeacherDashboard />} />
              <Route path="infrastructure-dashboard" element={<InfrastructureDashboard />} />
              <Route path="enrolment-dashboard" element={<EnrolmentDashboard />} />
              <Route path="dropbox-dashboard" element={<DropboxDashboard />} />
              <Route path="data-entry-dashboard" element={<DataEntryDashboard />} />
              <Route path="age-enrolment-dashboard" element={<AgeEnrolmentDashboard />} />
              <Route path="ctteacher-dashboard" element={<CTTeacherDashboard />} />
              <Route path="apaar-dashboard" element={<APAARDashboard />} />
              <Route path="classrooms-toilets-dashboard" element={<ClassroomsToiletsDashboard />} />
              <Route path="executive-dashboard" element={<ExecutiveDashboard />} />
              <Route path="analytics-dashboard" element={<AnalyticsDashboard />} />
              <Route path="user-management" element={
                <ProtectedRoute user={user} allowedRoles={["admin"]}>
                  <UserManagement />
                </ProtectedRoute>
              } />
              <Route path="district/:districtCode" element={<DistrictDetail />} />
              <Route path="block/:blockCode" element={<BlockDetail />} />
              <Route path="school/:udiseCode" element={<SchoolDetail />} />
            </Route>
            
            {/* Catch all - redirect to login or dashboard */}
            <Route path="*" element={<Navigate to={user ? "/executive-dashboard" : "/login"} replace />} />
          </Routes>
        </HashRouter>
      ) : (
        <BrowserRouter basename={ROUTER_BASENAME}>
          <Routes>
            {/* Public route */}
            <Route path="/login" element={
              user ? <Navigate to="/executive-dashboard" replace /> : <LoginPage onLogin={handleLogin} />
            } />
            
            {/* Protected routes */}
            <Route path="/" element={
              <ProtectedRoute user={user}>
                <DashboardLayout user={user} onLogout={handleLogout} />
              </ProtectedRoute>
            }>
              <Route index element={<StateOverview />} />
              <Route path="health-index" element={<SchoolHealthIndex />} />
              <Route path="data-import" element={
                <ProtectedRoute user={user} allowedRoles={["admin"]}>
                  <DataImport />
                </ProtectedRoute>
              } />
              <Route path="aadhaar-analytics" element={<AadhaarDashboard />} />
              <Route path="teacher-dashboard" element={<TeacherDashboard />} />
              <Route path="infrastructure-dashboard" element={<InfrastructureDashboard />} />
              <Route path="enrolment-dashboard" element={<EnrolmentDashboard />} />
              <Route path="dropbox-dashboard" element={<DropboxDashboard />} />
              <Route path="data-entry-dashboard" element={<DataEntryDashboard />} />
              <Route path="age-enrolment-dashboard" element={<AgeEnrolmentDashboard />} />
              <Route path="ctteacher-dashboard" element={<CTTeacherDashboard />} />
              <Route path="apaar-dashboard" element={<APAARDashboard />} />
              <Route path="classrooms-toilets-dashboard" element={<ClassroomsToiletsDashboard />} />
              <Route path="executive-dashboard" element={<ExecutiveDashboard />} />
              <Route path="analytics-dashboard" element={<AnalyticsDashboard />} />
              <Route path="user-management" element={
                <ProtectedRoute user={user} allowedRoles={["admin"]}>
                  <UserManagement />
                </ProtectedRoute>
              } />
              <Route path="district/:districtCode" element={<DistrictDetail />} />
              <Route path="block/:blockCode" element={<BlockDetail />} />
              <Route path="school/:udiseCode" element={<SchoolDetail />} />
            </Route>
            
            {/* Catch all - redirect to login or dashboard */}
            <Route path="*" element={<Navigate to={user ? "/executive-dashboard" : "/login"} replace />} />
          </Routes>
        </BrowserRouter>
      )}
      <Toaster position="top-right" />
    </div>
  );
}

export default App;
