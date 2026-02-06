import { useState, useEffect, useCallback } from "react";
import axios from "axios";
import { useScope } from "@/context/ScopeContext";
import { Card, CardContent, CardHeader, CardTitle, CardDescription } from "@/components/ui/card";
import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Progress } from "@/components/ui/progress";
import { Badge } from "@/components/ui/badge";
import { 
  Upload, 
  FileSpreadsheet, 
  CheckCircle2, 
  XCircle,
  Clock,
  RefreshCw,
  Database,
  Link,
  AlertCircle
} from "lucide-react";
import { toast } from "sonner";

import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;

const DataImport = () => {
  const { scope } = useScope();
  const [imports, setImports] = useState([]);
  const [stats, setStats] = useState(null);
  const [loading, setLoading] = useState(true);
  const [uploading, setUploading] = useState(false);
  const [urlInput, setUrlInput] = useState("");
  const [importingUrl, setImportingUrl] = useState(false);

  const fetchData = useCallback(async () => {
    try {
      const [importsRes, statsRes] = await Promise.all([
        axios.get(`${API}/import/list`),
        axios.get(`${API}/import/stats`)
      ]);
      setImports(importsRes.data);
      setStats(statsRes.data);
    } catch (error) {
      console.error("Error fetching import data:", error);
    } finally {
      setLoading(false);
    }
  }, [scope.version]);

  useEffect(() => {
    fetchData();
    const interval = setInterval(fetchData, 5000); // Poll every 5 seconds
    return () => clearInterval(interval);
  }, [fetchData]);

  const handleFileUpload = async (event) => {
    const file = event.target.files?.[0];
    if (!file) return;

    if (!file.name.endsWith('.xlsx') && !file.name.endsWith('.xls')) {
      toast.error("Please upload an Excel file (.xlsx or .xls)");
      return;
    }

    setUploading(true);
    const formData = new FormData();
    formData.append('file', file);

    try {
      const response = await axios.post(`${API}/import/upload`, formData, {
        headers: { 'Content-Type': 'multipart/form-data' }
      });
      toast.success(`File "${file.name}" uploaded successfully. Processing started.`);
      fetchData();
    } catch (error) {
      toast.error(`Upload failed: ${error.response?.data?.detail || error.message}`);
    } finally {
      setUploading(false);
      event.target.value = '';
    }
  };

  const handleUrlImport = async () => {
    if (!urlInput.trim()) {
      toast.error("Please enter a URL");
      return;
    }

    setImportingUrl(true);
    try {
      const response = await axios.post(`${API}/import/from-url?url=${encodeURIComponent(urlInput)}`);
      toast.success(`Import started for: ${response.data.filename}`);
      setUrlInput("");
      fetchData();
    } catch (error) {
      toast.error(`Import failed: ${error.response?.data?.detail || error.message}`);
    } finally {
      setImportingUrl(false);
    }
  };

  const getStatusBadge = (status) => {
    const variants = {
      pending: { icon: Clock, color: "bg-yellow-100 text-yellow-700" },
      processing: { icon: RefreshCw, color: "bg-blue-100 text-blue-700" },
      completed: { icon: CheckCircle2, color: "bg-green-100 text-green-700" },
      failed: { icon: XCircle, color: "bg-red-100 text-red-700" }
    };
    const { icon: Icon, color } = variants[status] || variants.pending;
    return (
      <Badge className={`${color} border-0`}>
        <Icon className={`w-3 h-3 mr-1 ${status === 'processing' ? 'animate-spin' : ''}`} />
        {status}
      </Badge>
    );
  };

  const getDatasetTypeLabel = (type) => {
    const labels = {
      aadhaar: "Aadhaar Status",
      apaar: "APAAR Entry",
      water: "Drinking Water",
      enrolment: "Enrolment",
      teacher: "Teacher Data",
      classroom: "Classroom/Toilet",
      data_entry: "Data Entry Status",
      comparison: "School Comparison",
      remarks: "Dropbox Remarks",
      age: "Age-wise Data"
    };
    return labels[type] || type || "Unknown";
  };

  if (loading) {
    return (
      <div className="flex items-center justify-center h-96">
        <div className="loading-spinner" />
      </div>
    );
  }

  return (
    <div className="space-y-6 animate-fade-in" data-testid="data-import">
      {/* Header */}
      <div className="flex items-center justify-between">
        <div>
          <h1 className="text-3xl font-bold text-slate-900 tracking-tight" style={{ fontFamily: 'Manrope' }}>
            Data Import
          </h1>
          <p className="text-slate-500 mt-1">Upload Excel files to import school data</p>
        </div>
        <Button variant="outline" size="sm" onClick={fetchData} data-testid="refresh-btn">
          <RefreshCw className="w-4 h-4 mr-2" />
          Refresh
        </Button>
      </div>

      {/* Stats Cards */}
      <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-4 gap-4">
        <Card className="border-slate-200">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-blue-100 rounded-lg">
                <Database className="w-5 h-5 text-blue-600" />
              </div>
              <div>
                <p className="text-2xl font-bold text-slate-900">{stats?.total_schools_in_db?.toLocaleString() || 0}</p>
                <p className="text-sm text-slate-500">Schools in Database</p>
              </div>
            </div>
          </CardContent>
        </Card>
        
        <Card className="border-slate-200">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-emerald-100 rounded-lg">
                <CheckCircle2 className="w-5 h-5 text-emerald-600" />
              </div>
              <div>
                <p className="text-2xl font-bold text-slate-900">{stats?.completed_imports || 0}</p>
                <p className="text-sm text-slate-500">Completed Imports</p>
              </div>
            </div>
          </CardContent>
        </Card>
        
        <Card className="border-slate-200">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <div className="p-2 bg-purple-100 rounded-lg">
                <FileSpreadsheet className="w-5 h-5 text-purple-600" />
              </div>
              <div>
                <p className="text-2xl font-bold text-slate-900">{stats?.unique_districts || 0}</p>
                <p className="text-sm text-slate-500">Districts</p>
              </div>
            </div>
          </CardContent>
        </Card>
        
        <Card className="border-slate-200">
          <CardContent className="pt-6">
            <div className="flex items-center gap-3">
              <div className={`p-2 rounded-lg ${stats?.data_source === 'imported' ? 'bg-green-100' : 'bg-amber-100'}`}>
                <AlertCircle className={`w-5 h-5 ${stats?.data_source === 'imported' ? 'text-green-600' : 'text-amber-600'}`} />
              </div>
              <div>
                <p className="text-2xl font-bold text-slate-900 capitalize">{stats?.data_source || 'Mock'}</p>
                <p className="text-sm text-slate-500">Data Source</p>
              </div>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Upload Section */}
      <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
        {/* File Upload */}
        <Card className="border-slate-200">
          <CardHeader>
            <CardTitle className="flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
              <Upload className="w-5 h-5 text-blue-600" />
              Upload Excel File
            </CardTitle>
            <CardDescription>
              Upload .xlsx or .xls files containing school data
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="border-2 border-dashed border-slate-200 rounded-lg p-8 text-center hover:border-slate-300 transition-colors">
              <input
                type="file"
                accept=".xlsx,.xls"
                onChange={handleFileUpload}
                className="hidden"
                id="file-upload"
                disabled={uploading}
                data-testid="file-upload-input"
              />
              <label htmlFor="file-upload" className="cursor-pointer">
                <FileSpreadsheet className="w-12 h-12 mx-auto text-slate-400 mb-4" />
                <p className="text-slate-600 mb-2">
                  {uploading ? "Uploading..." : "Click to upload or drag and drop"}
                </p>
                <p className="text-sm text-slate-400">
                  Supported formats: .xlsx, .xls
                </p>
              </label>
            </div>
          </CardContent>
        </Card>

        {/* URL Import */}
        <Card className="border-slate-200">
          <CardHeader>
            <CardTitle className="flex items-center gap-2" style={{ fontFamily: 'Manrope' }}>
              <Link className="w-5 h-5 text-purple-600" />
              Import from URL
            </CardTitle>
            <CardDescription>
              Import Excel file directly from a public URL
            </CardDescription>
          </CardHeader>
          <CardContent>
            <div className="space-y-4">
              <Input
                placeholder="Enter Excel file URL..."
                value={urlInput}
                onChange={(e) => setUrlInput(e.target.value)}
                disabled={importingUrl}
                data-testid="url-input"
              />
              <Button 
                onClick={handleUrlImport} 
                disabled={importingUrl || !urlInput.trim()}
                className="w-full"
                data-testid="import-url-btn"
              >
                {importingUrl ? (
                  <>
                    <RefreshCw className="w-4 h-4 mr-2 animate-spin" />
                    Importing...
                  </>
                ) : (
                  <>
                    <Upload className="w-4 h-4 mr-2" />
                    Import from URL
                  </>
                )}
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>

      {/* Supported Datasets */}
      <Card className="border-slate-200">
        <CardHeader>
          <CardTitle style={{ fontFamily: 'Manrope' }}>Supported Dataset Types</CardTitle>
        </CardHeader>
        <CardContent>
          <div className="grid grid-cols-2 md:grid-cols-5 gap-3">
            {[
              { type: "aadhaar", label: "Aadhaar Status" },
              { type: "apaar", label: "APAAR Entry" },
              { type: "water", label: "Drinking Water" },
              { type: "enrolment", label: "Enrolment" },
              { type: "teacher", label: "Teacher Data" },
              { type: "classroom", label: "Classroom/Toilet" },
              { type: "data_entry", label: "Data Entry" },
              { type: "comparison", label: "Comparison" },
              { type: "remarks", label: "Remarks" },
              { type: "age", label: "Age-wise" },
            ].map((item) => (
              <div key={item.type} className="p-3 bg-slate-50 rounded-lg text-center">
                <p className="text-sm font-medium text-slate-700">{item.label}</p>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      {/* Import History */}
      <Card className="border-slate-200">
        <CardHeader>
          <CardTitle style={{ fontFamily: 'Manrope' }}>Import History</CardTitle>
        </CardHeader>
        <CardContent>
          {imports.length === 0 ? (
            <div className="text-center py-8 text-slate-500">
              <FileSpreadsheet className="w-12 h-12 mx-auto mb-4 text-slate-300" />
              <p>No imports yet. Upload a file to get started.</p>
            </div>
          ) : (
            <div className="space-y-3">
              {imports.map((imp) => (
                <div 
                  key={imp.import_id} 
                  className="flex items-center justify-between p-4 bg-slate-50 rounded-lg"
                  data-testid={`import-item-${imp.import_id}`}
                >
                  <div className="flex items-center gap-4">
                    <FileSpreadsheet className="w-8 h-8 text-slate-400" />
                    <div>
                      <p className="font-medium text-slate-900">{imp.filename}</p>
                      <p className="text-sm text-slate-500">
                        {getDatasetTypeLabel(imp.dataset_type)} • {imp.records_processed?.toLocaleString() || 0} records
                      </p>
                    </div>
                  </div>
                  <div className="text-right">
                    {getStatusBadge(imp.status)}
                    <p className="text-xs text-slate-400 mt-1">
                      {new Date(imp.created_at).toLocaleString()}
                    </p>
                  </div>
                </div>
              ))}
            </div>
          )}
        </CardContent>
      </Card>
    </div>
  );
};

export default DataImport;
