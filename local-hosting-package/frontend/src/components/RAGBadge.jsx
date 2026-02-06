import { cn } from "@/lib/utils";

const RAGBadge = ({ status, label, size = "default" }) => {
  const colors = {
    green: "bg-emerald-100 text-emerald-700 border-emerald-200",
    amber: "bg-amber-100 text-amber-700 border-amber-200", 
    red: "bg-red-100 text-red-700 border-red-200",
  };
  
  const labels = {
    green: label || "Excellent",
    amber: label || "At Risk",
    red: label || "Critical",
  };
  
  const sizes = {
    small: "text-xs px-2 py-0.5",
    default: "text-xs px-2.5 py-1",
    large: "text-sm px-3 py-1.5",
  };

  return (
    <span 
      className={cn(
        "inline-flex items-center font-medium rounded-full border",
        colors[status] || colors.green,
        sizes[size]
      )}
      data-testid={`rag-badge-${status}`}
    >
      <span className={cn(
        "w-1.5 h-1.5 rounded-full mr-1.5",
        status === "green" && "bg-emerald-500",
        status === "amber" && "bg-amber-500",
        status === "red" && "bg-red-500"
      )} />
      {labels[status]}
    </span>
  );
};

export default RAGBadge;
