import React, { useState, useEffect, useMemo } from 'react';
import {
  ComposableMap,
  Geographies,
  Geography,
  ZoomableGroup
} from 'react-simple-maps';
import { scaleQuantize } from 'd3-scale';
import { Card, CardContent, CardHeader, CardTitle } from './ui/card';
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from './ui/select';
import { Badge } from './ui/badge';
import { MapPin, Info, TrendingUp, School, Users } from 'lucide-react';

// IMPORTANT: The app may be hosted under a sub-path (e.g. /MAHA-Education-Dashboard).
// Use PUBLIC_URL so the GeoJSON resolves correctly in both root + sub-path deployments.
const geoUrl = `${process.env.PUBLIC_URL || ""}/maharashtra-districts.geojson`;

// Color scales for different metrics
const colorScales = {
  shi: ["#fee2e2", "#fecaca", "#fca5a5", "#f87171", "#ef4444", "#dc2626", "#b91c1c", "#991b1b", "#7f1d1d"],
  aadhaar_coverage: ["#dcfce7", "#bbf7d0", "#86efac", "#4ade80", "#22c55e", "#16a34a", "#15803d", "#166534", "#14532d"],
  apaar_coverage: ["#dbeafe", "#bfdbfe", "#93c5fd", "#60a5fa", "#3b82f6", "#2563eb", "#1d4ed8", "#1e40af", "#1e3a8a"],
  infrastructure_index: ["#fef3c7", "#fde68a", "#fcd34d", "#fbbf24", "#f59e0b", "#d97706", "#b45309", "#92400e", "#78350f"],
  ctet_qualified_pct: ["#f3e8ff", "#e9d5ff", "#d8b4fe", "#c084fc", "#a855f7", "#9333ea", "#7e22ce", "#6b21a8", "#581c87"]
};

// Metric labels
const metricLabels = {
  shi: "School Health Index",
  aadhaar_coverage: "Aadhaar Coverage",
  apaar_coverage: "APAAR Generation",
  infrastructure_index: "Infrastructure Index",
  ctet_qualified_pct: "CTET Qualified Teachers"
};

const MaharashtraMap = ({ data, onDistrictClick }) => {
  const [selectedMetric, setSelectedMetric] = useState('shi');
  const [tooltipContent, setTooltipContent] = useState(null);
  const [tooltipPosition, setTooltipPosition] = useState({ x: 0, y: 0 });

  // Create a lookup map for district data
  const districtDataMap = useMemo(() => {
    if (!data?.districts) return {};
    const map = {};
    data.districts.forEach(d => {
      map[d.district_name] = d;
    });
    return map;
  }, [data]);

  // Create color scale based on selected metric
  const colorScale = useMemo(() => {
    if (!data?.districts) return null;
    
    const values = data.districts
      .filter(d => d.has_data && d.metrics[selectedMetric] !== null)
      .map(d => d.metrics[selectedMetric]);
    
    if (values.length === 0) return null;
    
    const min = Math.min(...values);
    const max = Math.max(...values);
    
    return scaleQuantize()
      .domain([min, max])
      .range(colorScales[selectedMetric] || colorScales.shi);
  }, [data, selectedMetric]);

  const getDistrictColor = (districtName) => {
    const districtData = districtDataMap[districtName];
    
    if (!districtData || !districtData.has_data) {
      return "#e5e7eb"; // Gray for no data
    }
    
    const value = districtData.metrics[selectedMetric];
    if (value === null || value === undefined || !colorScale) {
      return "#e5e7eb";
    }
    
    return colorScale(value);
  };

  const handleMouseEnter = (geo, evt) => {
    const districtName = geo.properties.district_name;
    const districtData = districtDataMap[districtName];
    
    setTooltipContent({
      name: districtName,
      data: districtData
    });
    setTooltipPosition({ x: evt.clientX, y: evt.clientY });
  };

  const handleMouseLeave = () => {
    setTooltipContent(null);
  };

  const handleClick = (geo) => {
    const districtName = geo.properties.district_name;
    const districtData = districtDataMap[districtName];
    
    if (districtData?.has_data && onDistrictClick) {
      onDistrictClick(districtData);
    }
  };

  if (!data) {
    return (
      <Card className="h-full">
        <CardContent className="flex items-center justify-center h-96">
          <div className="text-center text-gray-500">
            <MapPin className="w-12 h-12 mx-auto mb-2 animate-pulse" />
            <p>Loading map data...</p>
          </div>
        </CardContent>
      </Card>
    );
  }

  return (
    <Card className="h-full" data-testid="maharashtra-map">
      <CardHeader className="pb-2">
        <div className="flex items-center justify-between">
          <CardTitle className="text-lg font-semibold flex items-center gap-2">
            <MapPin className="w-5 h-5 text-blue-600" />
            Maharashtra District Map
          </CardTitle>
          <div className="flex items-center gap-4">
            <Select value={selectedMetric} onValueChange={setSelectedMetric}>
              <SelectTrigger className="w-[200px]" data-testid="metric-selector">
                <SelectValue placeholder="Select metric" />
              </SelectTrigger>
              <SelectContent>
                <SelectItem value="shi">School Health Index</SelectItem>
                <SelectItem value="aadhaar_coverage">Aadhaar Coverage</SelectItem>
                <SelectItem value="apaar_coverage">APAAR Generation</SelectItem>
                <SelectItem value="infrastructure_index">Infrastructure Index</SelectItem>
                <SelectItem value="ctet_qualified_pct">CTET Qualified %</SelectItem>
              </SelectContent>
            </Select>
          </div>
        </div>
        
        {/* Summary Stats */}
        <div className="flex gap-4 mt-2">
          <Badge variant="outline" className="text-xs">
            <School className="w-3 h-3 mr-1" />
            {data.summary?.districts_with_data || 0} / {data.summary?.total_districts || 36} Districts with Data
          </Badge>
          <Badge variant="secondary" className="text-xs">
            Avg {metricLabels[selectedMetric]}: {
              selectedMetric === 'shi' ? data.summary?.avg_shi :
              selectedMetric === 'aadhaar_coverage' ? data.summary?.avg_aadhaar :
              selectedMetric === 'apaar_coverage' ? data.summary?.avg_apaar : 'N/A'
            }%
          </Badge>
        </div>
      </CardHeader>
      
      <CardContent className="relative">
        {/* Map */}
        <div className="h-[500px] bg-gradient-to-br from-slate-50 to-slate-100 rounded-lg overflow-hidden border">
          <ComposableMap
            projection="geoMercator"
            projectionConfig={{
              scale: 4000,
              center: [76.5, 19.5]
            }}
            style={{ width: "100%", height: "100%" }}
          >
            <ZoomableGroup zoom={1} minZoom={0.8} maxZoom={4}>
              <Geographies geography={geoUrl}>
                {({ geographies }) =>
                  geographies.map((geo) => {
                    const districtName = geo.properties.district_name;
                    return (
                      <Geography
                        key={geo.rsmKey}
                        geography={geo}
                        fill={getDistrictColor(districtName)}
                        stroke="#fff"
                        strokeWidth={0.5}
                        style={{
                          default: {
                            outline: "none",
                            transition: "all 0.2s"
                          },
                          hover: {
                            fill: "#6366f1",
                            outline: "none",
                            cursor: districtDataMap[districtName]?.has_data ? "pointer" : "default"
                          },
                          pressed: {
                            fill: "#4f46e5",
                            outline: "none"
                          }
                        }}
                        onMouseEnter={(evt) => handleMouseEnter(geo, evt)}
                        onMouseLeave={handleMouseLeave}
                        onClick={() => handleClick(geo)}
                        data-testid={`district-${districtName?.toLowerCase().replace(/\s+/g, '-')}`}
                      />
                    );
                  })
                }
              </Geographies>
            </ZoomableGroup>
          </ComposableMap>
        </div>
        
        {/* Legend */}
        <div className="mt-4 flex items-center justify-between">
          <div className="flex items-center gap-2">
            <span className="text-xs text-gray-500">Low</span>
            <div className="flex">
              {colorScales[selectedMetric].map((color, i) => (
                <div
                  key={i}
                  className="w-6 h-4"
                  style={{ backgroundColor: color }}
                />
              ))}
            </div>
            <span className="text-xs text-gray-500">High</span>
          </div>
          <div className="flex items-center gap-4 text-xs">
            <div className="flex items-center gap-1">
              <div className="w-4 h-4 bg-gray-200 rounded"></div>
              <span className="text-gray-500">No Data</span>
            </div>
            <div className="flex items-center gap-1 text-blue-600">
              <Info className="w-3 h-3" />
              <span>Click district for details</span>
            </div>
          </div>
        </div>
        
        {/* Tooltip */}
        {tooltipContent && (
          <div
            className="fixed z-50 bg-white rounded-lg shadow-xl border p-3 pointer-events-none max-w-xs"
            style={{
              left: tooltipPosition.x + 10,
              top: tooltipPosition.y + 10
            }}
          >
            <div className="font-semibold text-gray-900 border-b pb-1 mb-2">
              {tooltipContent.name}
            </div>
            {tooltipContent.data?.has_data ? (
              <div className="space-y-1 text-sm">
                <div className="flex justify-between">
                  <span className="text-gray-500">Schools:</span>
                  <span className="font-medium">{tooltipContent.data.total_schools?.toLocaleString()}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Students:</span>
                  <span className="font-medium">{tooltipContent.data.total_students?.toLocaleString()}</span>
                </div>
                <div className="flex justify-between">
                  <span className="text-gray-500">Teachers:</span>
                  <span className="font-medium">{tooltipContent.data.total_teachers?.toLocaleString()}</span>
                </div>
                <div className="border-t pt-1 mt-1">
                  <div className="flex justify-between">
                    <span className="text-gray-500">{metricLabels[selectedMetric]}:</span>
                    <span className="font-semibold text-blue-600">
                      {tooltipContent.data.metrics[selectedMetric] !== null 
                        ? `${tooltipContent.data.metrics[selectedMetric]}%`
                        : 'N/A'}
                    </span>
                  </div>
                </div>
              </div>
            ) : (
              <div className="text-gray-400 text-sm italic">
                No data available for this district
              </div>
            )}
          </div>
        )}
      </CardContent>
    </Card>
  );
};

export default MaharashtraMap;
