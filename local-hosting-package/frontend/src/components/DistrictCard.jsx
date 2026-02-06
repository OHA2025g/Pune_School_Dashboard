import { useNavigate } from "react-router-dom";
import { cn } from "@/lib/utils";
import RAGBadge from "./RAGBadge";
import ProgressBar from "./ProgressBar";
import { ChevronRight, MapPin, Users, School } from "lucide-react";

const DistrictCard = ({ district, onClick }) => {
  const navigate = useNavigate();
  
  const handleClick = () => {
    if (onClick) {
      onClick(district);
    } else {
      navigate(`/district/${district.district_code}`);
    }
  };

  return (
    <div 
      className="district-card bg-white border border-slate-200 rounded-lg p-5 cursor-pointer hover:border-slate-300"
      onClick={handleClick}
      data-testid={`district-card-${district.district_code}`}
    >
      <div className="flex items-start justify-between mb-4">
        <div className="flex items-center gap-3">
          <div className="p-2 bg-slate-100 rounded-lg">
            <MapPin className="w-5 h-5 text-slate-600" strokeWidth={1.5} />
          </div>
          <div>
            <h3 className="font-semibold text-slate-900" style={{ fontFamily: 'Manrope' }}>
              {district.district_name}
            </h3>
            <p className="text-xs text-slate-500">Code: {district.district_code}</p>
          </div>
        </div>
        <RAGBadge status={district.rag_status} />
      </div>
      
      <div className="space-y-3">
        <div className="flex items-center justify-between text-sm">
          <span className="text-slate-500 flex items-center gap-1">
            <School className="w-4 h-4" /> Schools
          </span>
          <span className="font-medium text-slate-900 tabular-nums">
            {district.total_schools?.toLocaleString('en-IN')}
          </span>
        </div>
        
        <div className="flex items-center justify-between text-sm">
          <span className="text-slate-500 flex items-center gap-1">
            <Users className="w-4 h-4" /> Students
          </span>
          <span className="font-medium text-slate-900 tabular-nums">
            {district.total_students?.toLocaleString('en-IN')}
          </span>
        </div>
        
        <div className="pt-2 border-t border-slate-100">
          <div className="flex items-center justify-between mb-1">
            <span className="text-xs text-slate-500">SHI Score</span>
            <span className="text-xs font-medium text-slate-700">{district.shi_score}</span>
          </div>
          <ProgressBar value={district.shi_score} size="small" showLabel={false} />
        </div>
      </div>
      
      <div className="flex items-center justify-end mt-4 pt-3 border-t border-slate-100">
        <span className="text-sm text-blue-600 font-medium flex items-center gap-1">
          View Details
          <ChevronRight className="w-4 h-4" />
        </span>
      </div>
    </div>
  );
};

export default DistrictCard;
