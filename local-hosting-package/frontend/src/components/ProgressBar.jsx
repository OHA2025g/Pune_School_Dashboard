import { cn } from "@/lib/utils";

const ProgressBar = ({ value, max = 100, size = "default", showLabel = true, className }) => {
  const percentage = Math.min(100, Math.max(0, (value / max) * 100));
  
  const getColor = () => {
    if (percentage >= 85) return "bg-emerald-500";
    if (percentage >= 70) return "bg-amber-500";
    return "bg-red-500";
  };
  
  const sizes = {
    small: "h-1.5",
    default: "h-2",
    large: "h-3",
  };

  return (
    <div className={cn("flex items-center gap-3", className)}>
      <div className={cn("flex-1 bg-slate-100 rounded-full overflow-hidden", sizes[size])}>
        <div 
          className={cn("h-full rounded-full transition-all duration-500", getColor())}
          style={{ width: `${percentage}%` }}
        />
      </div>
      {showLabel && (
        <span className="text-sm font-medium text-slate-600 tabular-nums w-12 text-right">
          {value.toFixed(1)}%
        </span>
      )}
    </div>
  );
};

export default ProgressBar;
