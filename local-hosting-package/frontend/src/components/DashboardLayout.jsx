import React, { useState } from "react";
import { Outlet, NavLink, useLocation, useNavigate } from "react-router-dom";
import { 
  LayoutDashboard, 
  Users, 
  Building2, 
  GraduationCap, 
  Settings, 
  Trophy,
  Menu,
  X,
  Search,
  Bell,
  ChevronRight,
  Upload,
  ShieldCheck,
  UserCog,
  Droplets,
  BookOpen,
  FileText,
  FileCheck,
  CalendarDays,
  UserCheck,
  IdCard,
  Bath,
  BarChart3,
  LogOut,
  Brain
} from "lucide-react";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import ScopeFilter from "@/components/ScopeFilter";
import { ScopeProvider } from "@/context/ScopeContext";
import {
  Breadcrumb,
  BreadcrumbItem,
  BreadcrumbLink,
  BreadcrumbList,
  BreadcrumbSeparator,
} from "@/components/ui/breadcrumb";

const navItems = [
  { path: "/executive-dashboard", icon: BarChart3, label: "Executive Dashboard" },
  { path: "/analytics-dashboard", icon: Brain, label: "Advanced Analytics" },
  { path: "/", icon: LayoutDashboard, label: "State Overview", exact: true },
  { path: "/health-index", icon: Trophy, label: "School Health Index" },
  { path: "/aadhaar-analytics", icon: ShieldCheck, label: "Aadhaar Analytics" },
  { path: "/teacher-dashboard", icon: UserCog, label: "Teacher Dashboard" },
  { path: "/infrastructure-dashboard", icon: Droplets, label: "Water & Safety" },
  { path: "/enrolment-dashboard", icon: BookOpen, label: "Enrolment Analytics" },
  { path: "/dropbox-dashboard", icon: FileText, label: "Dropbox Remarks" },
  { path: "/data-entry-dashboard", icon: FileCheck, label: "Data Entry Status" },
  { path: "/age-enrolment-dashboard", icon: CalendarDays, label: "Age-wise Enrolment" },
  { path: "/ctteacher-dashboard", icon: UserCheck, label: "CTTeacher Analytics" },
  { path: "/apaar-dashboard", icon: IdCard, label: "APAAR Status" },
  { path: "/classrooms-toilets-dashboard", icon: Bath, label: "Classrooms & Toilets" },
  { path: "/data-import", icon: Upload, label: "Data Import", adminOnly: true },
  { path: "/user-management", icon: Users, label: "User Management", adminOnly: true },
];

const DashboardLayout = ({ user, onLogout }) => {
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const location = useLocation();
  const navigate = useNavigate();
  
  // Filter nav items based on user role
  const filteredNavItems = navItems.filter(item => {
    if (item.adminOnly && user?.role !== "admin") return false;
    return true;
  });

  const handleLogout = () => {
    if (onLogout) onLogout();
    navigate("/login");
  };
  
  const getBreadcrumbs = () => {
    const paths = location.pathname.split("/").filter(Boolean);
    const breadcrumbs = [{ label: "Maharashtra", path: "/" }];
    
    if (paths.length === 0) {
      breadcrumbs.push({ label: "State Overview", path: "/" });
    } else if (paths[0] === "district") {
      breadcrumbs.push({ label: "Districts", path: "/" });
      breadcrumbs.push({ label: paths[1], path: `/district/${paths[1]}` });
    } else if (paths[0] === "block") {
      breadcrumbs.push({ label: "Blocks", path: "/" });
      breadcrumbs.push({ label: paths[1], path: `/block/${paths[1]}` });
    } else if (paths[0] === "school") {
      breadcrumbs.push({ label: "Schools", path: "/" });
      breadcrumbs.push({ label: paths[1], path: `/school/${paths[1]}` });
    } else {
      const item = navItems.find(n => n.path === `/${paths[0]}`);
      if (item) {
        breadcrumbs.push({ label: item.label, path: item.path });
      }
    }
    
    return breadcrumbs;
  };

  return (
    <ScopeProvider>
      <div className="dashboard-layout">
      {/* Sidebar */}
      <aside className={`sidebar fixed inset-y-0 left-0 z-50 transform transition-transform duration-300 lg:translate-x-0 ${sidebarOpen ? 'translate-x-0' : '-translate-x-full'}`}>
        <div className="flex flex-col h-full">
          {/* Logo */}
          <div className="flex items-center justify-between px-4 py-5 border-b border-slate-800">
            <div className="flex items-center gap-3">
              <div className="w-10 h-10 rounded-lg bg-blue-600 flex items-center justify-center">
                <GraduationCap className="w-6 h-6 text-white" />
              </div>
              <div>
                <h1 className="text-lg font-bold text-white tracking-tight" style={{ fontFamily: 'Manrope' }}>
                  MH Education
                </h1>
                <p className="text-xs text-slate-400">Dashboard 2025-26</p>
              </div>
            </div>
            <button 
              className="lg:hidden text-slate-400 hover:text-white"
              onClick={() => setSidebarOpen(false)}
            >
              <X className="w-5 h-5" />
            </button>
          </div>
          
          {/* Navigation */}
          <nav className="flex-1 py-4 overflow-y-auto">
            <div className="px-3 mb-2">
              <span className="text-xs font-medium text-slate-500 uppercase tracking-wider">
                Analytics
              </span>
            </div>
            {filteredNavItems.map((item) => (
              <NavLink
                key={item.path}
                to={item.path}
                end={item.exact}
                className={({ isActive }) => 
                  `sidebar-item ${isActive ? 'active' : ''}`
                }
                data-testid={`nav-${item.label.toLowerCase().replace(/\s+/g, '-')}`}
              >
                <item.icon className="w-5 h-5" strokeWidth={1.5} />
                <span className="font-medium">{item.label}</span>
              </NavLink>
            ))}
          </nav>
          
          {/* Footer */}
          <div className="p-4 border-t border-slate-800">
            <div className="flex items-center gap-3">
              <div className="w-8 h-8 rounded-full bg-blue-600 flex items-center justify-center">
                <span className="text-sm font-medium text-white">{user?.full_name?.charAt(0) || 'U'}</span>
              </div>
              <div className="flex-1 min-w-0">
                <p className="text-sm font-medium text-white truncate">{user?.full_name || 'User'}</p>
                <p className="text-xs text-slate-400 capitalize">{user?.role?.replace('_', ' ') || 'Viewer'}</p>
              </div>
              <button
                onClick={handleLogout}
                className="p-2 text-slate-400 hover:text-white hover:bg-slate-700 rounded-lg transition-colors"
                title="Logout"
              >
                <LogOut className="w-4 h-4" />
              </button>
            </div>
          </div>
        </div>
      </aside>
      
      {/* Mobile Overlay */}
      {sidebarOpen && (
        <div 
          className="fixed inset-0 bg-black/50 z-40 lg:hidden"
          onClick={() => setSidebarOpen(false)}
        />
      )}
      
      {/* Main Content */}
      <main className="main-content">
        {/* Header */}
        <header className="dashboard-header px-4 lg:px-8 py-4">
          <div className="flex items-center justify-between gap-4">
            <div className="flex items-center gap-4">
              <Button
                variant="ghost"
                size="icon"
                className="lg:hidden"
                onClick={() => setSidebarOpen(true)}
                data-testid="mobile-menu-btn"
              >
                <Menu className="w-5 h-5" />
              </Button>
              
              <Breadcrumb>
                <BreadcrumbList>
                  {getBreadcrumbs().map((crumb, idx, arr) => (
                    <React.Fragment key={idx}>
                      <BreadcrumbItem>
                        <BreadcrumbLink href={crumb.path} className="text-slate-600 hover:text-slate-900">
                          {crumb.label}
                        </BreadcrumbLink>
                      </BreadcrumbItem>
                      {idx < arr.length - 1 && (
                        <BreadcrumbSeparator>
                          <ChevronRight className="w-4 h-4" />
                        </BreadcrumbSeparator>
                      )}
                    </React.Fragment>
                  ))}
                </BreadcrumbList>
              </Breadcrumb>
            </div>
            
            <div className="flex items-center gap-3">
              <div className="hidden lg:block">
                <ScopeFilter />
              </div>
              <div className="relative hidden md:block">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-slate-400" />
                <Input 
                  placeholder="Search schools, districts..." 
                  className="pl-9 w-64 bg-slate-50 border-slate-200"
                  data-testid="search-input"
                />
              </div>
              <Button variant="ghost" size="icon" className="relative" data-testid="notifications-btn">
                <Bell className="w-5 h-5" />
                <span className="absolute top-1 right-1 w-2 h-2 bg-red-500 rounded-full" />
              </Button>
            </div>
          </div>
          <div className="mt-3 lg:hidden">
            <div className="flex items-center gap-2">
              <div className="flex-1 min-w-0">
                <ScopeFilter />
              </div>
            </div>
          </div>
        </header>

        
        {/* Page Content */}
        <div className="p-4 lg:p-8">
          <Outlet />
        </div>
      </main>
      </div>
    </ScopeProvider>
  );
};

export default DashboardLayout;
