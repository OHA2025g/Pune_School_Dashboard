import { TrendingUp, TrendingDown, Minus } from "lucide-react";
import { cn } from "@/lib/utils";

const KPICard = ({ 
  label, 
  value, 
  suffix = "", 
  trend = null, 
  trendValue = null,
  icon: Icon,
  className,
  onClick,
  testId
}) => {
  const getTrendIcon = () => {
    if (trend === "up") return <TrendingUp className="w-4 h-4 text-emerald-500" />;
    if (trend === "down") return <TrendingDown className="w-4 h-4 text-red-500" />;
    return <Minus className="w-4 h-4 text-slate-400" />;
  };
  
  const getTrendColor = () => {
    if (trend === "up") return "text-emerald-600";
    if (trend === "down") return "text-red-600";
    return "text-slate-500";
  };

  return (
    <div 
      className={cn(
        "kpi-card group cursor-pointer",
        className
      )}
      onClick={onClick}
      data-testid={testId}
    >
      <div className="flex items-start justify-between">
        <div className="flex-1">
          <p className="kpi-label mb-2">{label}</p>
          <div className="flex items-baseline gap-2">
            <span className="kpi-value">
              {typeof value === 'number' ? value.toLocaleString('en-IN') : value}
            </span>
            {suffix && (
              <span className="text-lg font-semibold text-slate-500">{suffix}</span>
            )}
          </div>
          {trend && trendValue && (
            <div className={cn("flex items-center gap-1 mt-2", getTrendColor())}>
              {getTrendIcon()}
              <span className="text-sm font-medium">{trendValue}</span>
            </div>
          )}
        </div>
        {Icon && (
          <div className="p-2 bg-slate-100 rounded-lg group-hover:bg-slate-200 transition-colors">
            <Icon className="w-5 h-5 text-slate-600" strokeWidth={1.5} />
          </div>
        )}
      </div>
    </div>
  );
};

export default KPICard;
