import { useEffect, useMemo, useState } from "react";
import axios from "axios";
import { Building2, GraduationCap, MapPin, X } from "lucide-react";
import { Button } from "@/components/ui/button";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "@/components/ui/select";
import { useScope } from "@/context/ScopeContext";
import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;
const ALL_VALUE = "__all__";

export default function ScopeFilter() {
  const { scope, setScope, clearScope } = useScope();
  const [districts, setDistricts] = useState([]);
  const [blocks, setBlocks] = useState([]);
  const [schools, setSchools] = useState([]);

  const selectedDistrict = useMemo(
    () => districts.find((d) => d.district_code === scope.districtCode),
    [districts, scope.districtCode],
  );
  const selectedBlock = useMemo(
    () => blocks.find((b) => b.block_code === scope.blockCode),
    [blocks, scope.blockCode],
  );
  const selectedSchool = useMemo(
    () => schools.find((s) => s.udise_code === scope.udiseCode),
    [schools, scope.udiseCode],
  );

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const res = await axios.get(`${API}/scope/districts`, {
          headers: { "x-skip-scope": "true" },
        });
        if (!cancelled) setDistricts(res.data || []);
      } catch {
        if (!cancelled) setDistricts([]);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, []);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      if (!scope.districtCode) {
        setBlocks([]);
        return;
      }
      try {
        const res = await axios.get(`${API}/scope/districts/${scope.districtCode}/blocks`, {
          headers: { "x-skip-scope": "true" },
        });
        if (!cancelled) setBlocks(res.data || []);
      } catch {
        if (!cancelled) setBlocks([]);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [scope.districtCode]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      if (!scope.blockCode) {
        setSchools([]);
        return;
      }
      try {
        const res = await axios.get(`${API}/scope/blocks/${scope.blockCode}/schools?limit=500`, {
          headers: { "x-skip-scope": "true" },
        });
        if (!cancelled) setSchools(res.data || []);
      } catch {
        if (!cancelled) setSchools([]);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [scope.blockCode]);

  const onDistrictChange = (districtCode) => {
    if (!districtCode || districtCode === ALL_VALUE) {
      clearScope();
      return;
    }
    const d = districts.find((x) => x.district_code === districtCode);
    setScope({
      districtCode,
      districtName: d?.district_name || "",
      blockCode: "",
      blockName: "",
      udiseCode: "",
      schoolName: "",
    });
  };

  const onBlockChange = (blockCode) => {
    if (!blockCode || blockCode === ALL_VALUE) {
      setScope({
        blockCode: "",
        blockName: "",
        udiseCode: "",
        schoolName: "",
      });
      return;
    }
    const b = blocks.find((x) => x.block_code === blockCode);
    setScope({
      blockCode,
      blockName: b?.block_name || "",
      udiseCode: "",
      schoolName: "",
    });
  };

  const onSchoolChange = (udiseCode) => {
    if (!udiseCode || udiseCode === ALL_VALUE) {
      setScope({ udiseCode: "", schoolName: "" });
      return;
    }
    const s = schools.find((x) => x.udise_code === udiseCode);
    setScope({ udiseCode, schoolName: s?.school_name || "" });
  };

  return (
    <div className="flex flex-wrap items-center gap-2">
      <div className="flex items-center gap-2 text-xs text-slate-500">
        <MapPin className="w-4 h-4" />
        <span className="hidden lg:inline">Scope</span>
      </div>

      <Select value={scope.districtCode || ""} onValueChange={onDistrictChange}>
        <SelectTrigger className="w-[220px] bg-white" data-testid="scope-district">
          <SelectValue placeholder="District (All)" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ALL_VALUE}>All Districts</SelectItem>
          {districts.map((d) => (
            <SelectItem key={d.district_code} value={d.district_code}>
              <span className="flex items-center gap-2">
                <Building2 className="w-4 h-4" />
                {d.district_name}
              </span>
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      <Select value={scope.blockCode || ""} onValueChange={onBlockChange} disabled={!scope.districtCode}>
        <SelectTrigger className="w-[220px] bg-white" data-testid="scope-block">
          <SelectValue placeholder="Block (All)" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ALL_VALUE}>All Blocks</SelectItem>
          {blocks.map((b) => (
            <SelectItem key={b.block_code} value={b.block_code}>
              <span className="flex items-center gap-2">
                <Building2 className="w-4 h-4" />
                {b.block_name}
              </span>
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      <Select value={scope.udiseCode || ""} onValueChange={onSchoolChange} disabled={!scope.blockCode}>
        <SelectTrigger className="w-[260px] bg-white" data-testid="scope-school">
          <SelectValue placeholder="School (All)" />
        </SelectTrigger>
        <SelectContent>
          <SelectItem value={ALL_VALUE}>All Schools</SelectItem>
          {schools.map((s) => (
            <SelectItem key={s.udise_code} value={s.udise_code}>
              <span className="flex items-center gap-2">
                <GraduationCap className="w-4 h-4" />
                <span className="line-clamp-1">{s.school_name}</span>
              </span>
            </SelectItem>
          ))}
        </SelectContent>
      </Select>

      {(scope.districtCode || scope.blockCode || scope.udiseCode) && (
        <Button
          variant="ghost"
          size="sm"
          className="text-slate-600"
          onClick={clearScope}
          title="Clear filters"
          data-testid="scope-clear"
        >
          <X className="w-4 h-4 mr-1" />
          Clear
        </Button>
      )}

      {/* Compact selection hint */}
      <div className="hidden xl:block text-xs text-slate-500">
        {selectedDistrict?.district_name && <span>District: {selectedDistrict.district_name}</span>}
        {selectedBlock?.block_name && <span> • Block: {selectedBlock.block_name}</span>}
        {selectedSchool?.school_name && <span> • School: {selectedSchool.school_name}</span>}
      </div>
    </div>
  );
}


