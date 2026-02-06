import { useState } from "react";
import axios from "axios";
import { Button } from "@/components/ui/button";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuTrigger, DropdownMenuSeparator } from "@/components/ui/dropdown-menu";
import { toast } from "sonner";
import { Download, FileSpreadsheet, FileText, Loader2 } from "lucide-react";
import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();

const ExportPanel = ({ dashboardName, dashboardTitle }) => {
  const [exporting, setExporting] = useState(null);
  
  // Check if user can export
  const user = JSON.parse(localStorage.getItem("user") || "{}");
  const canExport = user.permissions?.can_export !== false && user.role !== "viewer";

  const handleExport = async (format) => {
    if (!canExport) {
      toast.error("You don't have permission to export data");
      return;
    }

    setExporting(format);
    try {
      const endpoint = format === "excel" 
        ? `${BACKEND_URL}/api/export/excel/${dashboardName}`
        : `${BACKEND_URL}/api/export/pdf/${dashboardName}`;
      
      const response = await axios.get(endpoint, {
        responseType: "blob"
      });
      
      // Create download link
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement("a");
      link.href = url;
      link.setAttribute("download", `${dashboardName}_${new Date().toISOString().split('T')[0]}.${format === "excel" ? "xlsx" : "pdf"}`);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
      
      toast.success(`${dashboardTitle} exported successfully!`);
    } catch (error) {
      console.error("Export error:", error);
      toast.error("Export failed. Please try again.");
    } finally {
      setExporting(null);
    }
  };

  const handleExportExecutive = async (format) => {
    if (!canExport) {
      toast.error("You don't have permission to export data");
      return;
    }

    setExporting(`exec-${format}`);
    try {
      const endpoint = format === "excel"
        ? `${BACKEND_URL}/api/export/excel/executive-summary`
        : `${BACKEND_URL}/api/export/pdf/executive-summary`;
      
      const response = await axios.get(endpoint, {
        responseType: "blob"
      });
      
      const url = window.URL.createObjectURL(new Blob([response.data]));
      const link = document.createElement("a");
      link.href = url;
      link.setAttribute("download", `executive_summary_${new Date().toISOString().split('T')[0]}.${format === "excel" ? "xlsx" : "pdf"}`);
      document.body.appendChild(link);
      link.click();
      link.remove();
      window.URL.revokeObjectURL(url);
      
      toast.success("Executive Summary exported successfully!");
    } catch (error) {
      toast.error("Export failed. Please try again.");
    } finally {
      setExporting(null);
    }
  };

  if (!canExport) {
    return null;
  }

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" data-testid="export-btn">
          {exporting ? (
            <Loader2 className="w-4 h-4 mr-2 animate-spin" />
          ) : (
            <Download className="w-4 h-4 mr-2" />
          )}
          Export
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end" className="w-48">
        <DropdownMenuItem onClick={() => handleExport("excel")} disabled={!!exporting}>
          <FileSpreadsheet className="w-4 h-4 mr-2 text-green-600" />
          Export to Excel
        </DropdownMenuItem>
        <DropdownMenuItem onClick={() => handleExport("pdf")} disabled={!!exporting}>
          <FileText className="w-4 h-4 mr-2 text-red-600" />
          Export to PDF
        </DropdownMenuItem>
        
        {dashboardName !== "executive-summary" && (
          <>
            <DropdownMenuSeparator />
            <DropdownMenuItem onClick={() => handleExportExecutive("excel")} disabled={!!exporting}>
              <FileSpreadsheet className="w-4 h-4 mr-2 text-blue-600" />
              Full Report (Excel)
            </DropdownMenuItem>
            <DropdownMenuItem onClick={() => handleExportExecutive("pdf")} disabled={!!exporting}>
              <FileText className="w-4 h-4 mr-2 text-purple-600" />
              Full Report (PDF)
            </DropdownMenuItem>
          </>
        )}
      </DropdownMenuContent>
    </DropdownMenu>
  );
};

export default ExportPanel;
