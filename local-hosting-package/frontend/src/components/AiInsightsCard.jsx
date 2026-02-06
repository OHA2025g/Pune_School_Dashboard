import { Accordion, AccordionContent, AccordionItem, AccordionTrigger } from "@/components/ui/accordion";
import { Card, CardContent, CardHeader, CardTitle } from "@/components/ui/card";
import { Loader2, Sparkles } from "lucide-react";
import ReactMarkdown from "react-markdown";

const _bucketForHeading = (heading) => {
  const h = String(heading || "").toLowerCase();
  if (h.includes("root cause")) return "root";
  if (h.includes("priority action") || h.includes("action item") || h.includes("priority")) return "actions";
  if (h.includes("recommend")) return "recs";
  return "insights";
};

const splitIntoInsightBuckets = (markdown) => {
  const text = String(markdown || "").trim();
  const buckets = { insights: "", root: "", recs: "", actions: "" };
  if (!text) return buckets;

  // Parse by "## " headings. If none, put all into Insights.
  const re = /^##\s+(.+)$/gm;
  const matches = [];
  let m;
  while ((m = re.exec(text)) !== null) {
    matches.push({ title: m[1].trim(), index: m.index });
  }
  if (matches.length === 0) {
    buckets.insights = text;
    return buckets;
  }

  for (let i = 0; i < matches.length; i++) {
    const cur = matches[i];
    const next = matches[i + 1];
    const start = cur.index;
    const end = next ? next.index : text.length;
    const block = text.slice(start, end).trim();
    // Turn the original H2 into H3 so the accordion itself is the main structure.
    const normalized = block.replace(/^##\s+/m, "### ");
    const bucket = _bucketForHeading(cur.title);
    buckets[bucket] = (buckets[bucket] ? `${buckets[bucket]}\n\n` : "") + normalized;
  }

  return buckets;
};

export const CollapsibleInsights = ({ content }) => {
  const buckets = splitIntoInsightBuckets(content);

  const panels = [
    { key: "insights", title: "Insights", body: buckets.insights },
    { key: "root", title: "Root Cause Signals", body: buckets.root },
    { key: "recs", title: "Recommendations", body: buckets.recs },
    { key: "actions", title: "Priority Action Items", body: buckets.actions },
  ];

  const markdownClass =
    "prose prose-sm md:prose-base max-w-none prose-slate " +
    "prose-headings:font-semibold prose-headings:text-slate-900 " +
    "prose-strong:text-slate-900 prose-li:my-1 prose-ul:my-2 prose-ol:my-2 " +
    "prose-a:text-blue-700 prose-a:no-underline hover:prose-a:underline";

  return (
    <Accordion type="multiple" defaultValue={["insights"]} className="w-full">
      {panels.map((p) => (
        <AccordionItem key={p.key} value={p.key} className="border-slate-200">
          <AccordionTrigger className="text-slate-900 hover:no-underline">
            <div className="flex items-center gap-2">
              <span className="text-sm font-semibold">{p.title}</span>
              {!p.body ? <span className="text-xs font-normal text-slate-500">(no content)</span> : null}
            </div>
          </AccordionTrigger>
          <AccordionContent>
            {p.body ? (
              <div className={markdownClass}>
                <ReactMarkdown>{p.body}</ReactMarkdown>
              </div>
            ) : (
              <div className="text-sm text-slate-500">No content available for this section.</div>
            )}
          </AccordionContent>
        </AccordionItem>
      ))}
    </Accordion>
  );
};

export default function AiInsightsCard({ title = "AI Insights", content, loading }) {
  if (loading) {
    return (
      <Card className="border-slate-200 bg-gradient-to-br from-purple-50 to-blue-50">
        <CardContent className="p-6 flex items-center justify-center h-56">
          <div className="flex flex-col items-center gap-3">
            <Loader2 className="w-8 h-8 animate-spin text-purple-600" />
            <p className="text-slate-500">Generating insights…</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="border-slate-200 bg-gradient-to-br from-purple-50 to-blue-50">
      <CardHeader className="pb-2">
        <CardTitle className="text-lg flex items-center gap-2">
          <Sparkles className="w-5 h-5 text-purple-600" />
          {title}
        </CardTitle>
      </CardHeader>
      <CardContent>
        <div className="max-h-96 overflow-y-auto pr-2">
          <CollapsibleInsights content={content || ""} />
        </div>
      </CardContent>
    </Card>
  );
}


