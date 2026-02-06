import { useEffect, useState, useCallback, useRef } from "react";
import axios from "axios";
import { Brain, Loader2 } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Card, CardContent } from "@/components/ui/card";
import { toast } from "sonner";
import AiInsightsCard from "@/components/AiInsightsCard";
import { getBackendUrl } from "@/lib/backend";
import { useScope } from "@/context/ScopeContext";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;

export default function AiInsightsTab({
  title = "AI Insights",
  description = "Data-driven insights based on the selected filters",
  endpoint = "/analytics/insights/executive-summary",
  generate,
  autoGenerate = false,
  dataDependency = null, // Pass overview or other data to watch for changes
}) {
  const { scope } = useScope();
  const [content, setContent] = useState("");
  const [loading, setLoading] = useState(false);
  const [hasGenerated, setHasGenerated] = useState(false);
  const prevDataKeyRef = useRef(null);

  const fetchInsights = useCallback(async () => {
    setLoading(true);
    try {
      let text = "";
      if (typeof generate === "function") {
        text = generate() || "";
      } else {
        const response = await axios.get(`${API}${endpoint}`);
        text =
          response?.data?.ai_summary ||
          response?.data?.ai_insights ||
          response?.data?.summary ||
          response?.data?.insights ||
          "";
      }
      setContent(text || "No data available to generate insights.");
      setHasGenerated(true);
      if (!autoGenerate) {
        toast.success("Insights generated.");
      }
    } catch (error) {
      const detail = error?.response?.data?.detail || error?.message || "Failed to generate insights.";
      if (!autoGenerate) {
        toast.error(detail);
      }
      setContent("");
      setHasGenerated(false);
    } finally {
      setLoading(false);
    }
  }, [generate, endpoint, autoGenerate]);

  // Clear content when scope changes
  useEffect(() => {
    setContent("");
    setHasGenerated(false);
    prevDataKeyRef.current = null; // Reset on scope change
  }, [scope.version]);

  // Auto-generate insights when data changes
  useEffect(() => {
    if (!autoGenerate || typeof generate !== "function" || !dataDependency) {
      return;
    }

    // Check if data is actually available (not just null/empty)
    const hasValidData = typeof dataDependency === 'object' &&
      (dataDependency.total_teachers > 0 || 
       dataDependency.total_schools > 0 || 
       Object.keys(dataDependency).length > 3);
    
    if (!hasValidData) {
      return;
    }

    // Create a key to detect if data actually changed
    const dataKey = JSON.stringify({
      total_teachers: dataDependency.total_teachers,
      total_schools: dataDependency.total_schools,
      scope_version: scope.version
    });

    // Only regenerate if data key changed
    if (dataKey !== prevDataKeyRef.current) {
      prevDataKeyRef.current = dataKey;
      
      // Small delay to ensure all data has loaded
      const timer = setTimeout(() => {
        try {
          // Try to generate - if data is available, it will work
          const result = generate();
          if (result && result.trim() && !result.includes("No CTTeacher data available")) {
            fetchInsights();
          }
        } catch (error) {
          // If generation fails (no data), don't auto-generate
          console.debug("Auto-generate skipped - data not ready:", error);
        }
      }, 1500); // Delay to ensure data is fully loaded
      return () => clearTimeout(timer);
    }
  }, [dataDependency, generate, autoGenerate, fetchInsights, scope.version]);

  return (
    <div className="space-y-6">
      <Card className="border-slate-200">
        <CardContent className="py-8 text-center">
          <Brain className="w-12 h-12 mx-auto text-slate-300 mb-3" />
          <h3 className="text-lg font-semibold text-slate-700 mb-2">{title}</h3>
          <p className="text-slate-500 mb-4">{description}</p>
          <Button onClick={fetchInsights} disabled={loading}>
            {loading ? (
              <>
                <Loader2 className="w-4 h-4 mr-2 animate-spin" />
                Generating...
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

      <AiInsightsCard title={title} content={content} loading={loading} />
    </div>
  );
}
