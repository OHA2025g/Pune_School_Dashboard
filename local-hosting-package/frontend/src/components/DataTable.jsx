import { useNavigate } from "react-router-dom";
import {
  Table,
  TableBody,
  TableCell,
  TableHead,
  TableHeader,
  TableRow,
} from "@/components/ui/table";
import RAGBadge from "./RAGBadge";
import ProgressBar from "./ProgressBar";
import { ChevronRight } from "lucide-react";

const DataTable = ({ 
  data, 
  columns, 
  onRowClick,
  sortBy,
  sortOrder,
  onSort,
  testId = "data-table"
}) => {
  const navigate = useNavigate();
  
  const handleRowClick = (row) => {
    if (onRowClick) {
      onRowClick(row);
    }
  };
  
  const renderCell = (row, column) => {
    const value = row[column.key];
    
    switch (column.type) {
      case "rag":
        return <RAGBadge status={value} />;
      case "progress":
        return <ProgressBar value={value} showLabel={true} size="small" />;
      case "number":
        return (
          <span className="tabular-nums">
            {typeof value === 'number' ? value.toLocaleString('en-IN') : value}
          </span>
        );
      case "percentage":
        return (
          <span className="tabular-nums">
            {typeof value === 'number' ? `${value.toFixed(1)}%` : value}
          </span>
        );
      case "action":
        return (
          <button className="p-2 hover:bg-slate-100 rounded-lg transition-colors">
            <ChevronRight className="w-4 h-4 text-slate-400" />
          </button>
        );
      default:
        return value;
    }
  };
  
  const handleSort = (column) => {
    if (column.sortable && onSort) {
      const newOrder = sortBy === column.key && sortOrder === "desc" ? "asc" : "desc";
      onSort(column.key, newOrder);
    }
  };

  return (
    <div className="bg-white border border-slate-200 rounded-lg overflow-hidden" data-testid={testId}>
      <Table>
        <TableHeader>
          <TableRow className="bg-slate-50 hover:bg-slate-50">
            {columns.map((column) => (
              <TableHead 
                key={column.key}
                className={`text-xs font-medium text-slate-500 uppercase tracking-wider ${column.sortable ? 'cursor-pointer hover:text-slate-700' : ''} ${column.align === 'right' ? 'text-right' : ''}`}
                onClick={() => handleSort(column)}
              >
                <div className="flex items-center gap-1">
                  {column.label}
                  {column.sortable && sortBy === column.key && (
                    <span className="text-blue-600">
                      {sortOrder === "desc" ? "↓" : "↑"}
                    </span>
                  )}
                </div>
              </TableHead>
            ))}
          </TableRow>
        </TableHeader>
        <TableBody>
          {data.map((row, idx) => (
            <TableRow 
              key={idx}
              className="cursor-pointer hover:bg-slate-50/50 transition-colors"
              onClick={() => handleRowClick(row)}
              data-testid={`table-row-${idx}`}
            >
              {columns.map((column) => (
                <TableCell 
                  key={column.key}
                  className={`${column.align === 'right' ? 'text-right' : ''}`}
                >
                  {renderCell(row, column)}
                </TableCell>
              ))}
            </TableRow>
          ))}
        </TableBody>
      </Table>
    </div>
  );
};

export default DataTable;
