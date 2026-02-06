import { useEffect, useMemo, useState } from "react";
import axios from "axios";
import { useNavigate } from "react-router-dom";
import { Building2, ChevronRight, GraduationCap, MapPin, Search, ArrowLeft } from "lucide-react";

import { Button } from "@/components/ui/button";
import { Input } from "@/components/ui/input";
import { Dialog, DialogContent, DialogHeader, DialogTitle } from "@/components/ui/dialog";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "@/components/ui/table";
import { useScope } from "@/context/ScopeContext";
import { getBackendUrl } from "@/lib/backend";

const BACKEND_URL = getBackendUrl();
const API = `${BACKEND_URL}/api`;

function useDebounced(value, delayMs) {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const t = setTimeout(() => setDebounced(value), delayMs);
    return () => clearTimeout(t);
  }, [value, delayMs]);
  return debounced;
}

export default function DrilldownExplorer({ open, onOpenChange }) {
  const navigate = useNavigate();
  const { scope, setScope, clearScope } = useScope();

  const [level, setLevel] = useState("district"); // district | block | school
  const [districts, setDistricts] = useState([]);
  const [blocks, setBlocks] = useState([]);
  const [schools, setSchools] = useState([]);

  const [loading, setLoading] = useState(false);
  const [schoolQuery, setSchoolQuery] = useState("");
  const debouncedSchoolQuery = useDebounced(schoolQuery, 250);

  const selectedDistrict = useMemo(
    () => districts.find((d) => d.district_code === scope.districtCode),
    [districts, scope.districtCode],
  );
  const selectedBlock = useMemo(
    () => blocks.find((b) => b.block_code === scope.blockCode),
    [blocks, scope.blockCode],
  );

  // Reset to district view when opening (predictable UX)
  useEffect(() => {
    if (open) {
      setLevel("district");
      setSchoolQuery("");
    }
  }, [open]);

  // Load districts once per open
  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    (async () => {
      setLoading(true);
      try {
        const res = await axios.get(`${API}/scope/districts`, { headers: { "x-skip-scope": "true" } });
        if (!cancelled) setDistricts(res.data || []);
      } catch {
        if (!cancelled) setDistricts([]);
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [open]);

  // Load blocks when district scope changes (and modal open)
  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    (async () => {
      if (!scope.districtCode) {
        setBlocks([]);
        return;
      }
      setLoading(true);
      try {
        const res = await axios.get(`${API}/scope/districts/${scope.districtCode}/blocks`, {
          headers: { "x-skip-scope": "true" },
        });
        if (!cancelled) setBlocks(res.data || []);
      } catch {
        if (!cancelled) setBlocks([]);
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [open, scope.districtCode]);

  // Load schools when block scope changes OR query changes
  useEffect(() => {
    if (!open) return;
    let cancelled = false;
    (async () => {
      if (!scope.blockCode) {
        setSchools([]);
        return;
      }
      setLoading(true);
      try {
        const q = debouncedSchoolQuery?.trim();
        const url = q
          ? `${API}/scope/blocks/${scope.blockCode}/schools?limit=500&q=${encodeURIComponent(q)}`
          : `${API}/scope/blocks/${scope.blockCode}/schools?limit=500`;
        const res = await axios.get(url, { headers: { "x-skip-scope": "true" } });
        if (!cancelled) setSchools(res.data || []);
      } catch {
        if (!cancelled) setSchools([]);
      } finally {
        if (!cancelled) setLoading(false);
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [open, scope.blockCode, debouncedSchoolQuery]);

  const breadcrumb = useMemo(() => {
    const items = [];
    items.push({ key: "district", label: selectedDistrict?.district_name || "District" });
    if (scope.districtCode) items.push({ key: "block", label: selectedBlock?.block_name || "Block" });
    if (scope.blockCode) items.push({ key: "school", label: "School" });
    return items;
  }, [selectedDistrict?.district_name, selectedBlock?.block_name, scope.blockCode, scope.districtCode]);

  const goBack = () => {
    if (level === "school") setLevel("block");
    else if (level === "block") setLevel("district");
  };

  const selectDistrict = (d) => {
    setScope({
      districtCode: d.district_code,
      districtName: d.district_name,
      blockCode: "",
      blockName: "",
      udiseCode: "",
      schoolName: "",
    });
    setLevel("block");
  };

  const selectBlock = (b) => {
    setScope({
      blockCode: b.block_code,
      blockName: b.block_name,
      udiseCode: "",
      schoolName: "",
    });
    setLevel("school");
  };

  const selectSchool = (s) => {
    setScope({ udiseCode: s.udise_code, schoolName: s.school_name });
  };

  const openCurrentView = () => {
    if (scope.udiseCode) navigate(`/school/${scope.udiseCode}`);
    else if (scope.blockCode) navigate(`/block/${scope.blockCode}`);
    else if (scope.districtCode) navigate(`/district/${scope.districtCode}`);
    onOpenChange(false);
  };

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-3xl">
        <DialogHeader>
          <DialogTitle className="flex items-center justify-between gap-3">
            <span className="flex items-center gap-2">
              <MapPin className="w-5 h-5 text-slate-700" />
              Drilldown Explorer
            </span>
            <div className="flex items-center gap-2">
              <Button variant="outline" size="sm" onClick={clearScope} disabled={loading}>
                Clear
              </Button>
              <Button size="sm" onClick={openCurrentView} disabled={loading || (!scope.districtCode && !scope.blockCode && !scope.udiseCode)}>
                Open View
                <ChevronRight className="w-4 h-4 ml-1" />
              </Button>
            </div>
          </DialogTitle>
        </DialogHeader>

        <div className="flex items-center gap-2 text-sm text-slate-600">
          <Button variant="ghost" size="sm" onClick={goBack} disabled={loading || level === "district"}>
            <ArrowLeft className="w-4 h-4 mr-1" />
            Back
          </Button>
          <div className="flex items-center gap-2">
            {breadcrumb.map((b, idx) => (
              <span key={b.key} className="flex items-center gap-2">
                <span className="font-medium">{b.label}</span>
                {idx < breadcrumb.length - 1 ? <ChevronRight className="w-4 h-4 text-slate-400" /> : null}
              </span>
            ))}
          </div>
        </div>

        {level === "school" ? (
          <div className="flex items-center gap-2">
            <Search className="w-4 h-4 text-slate-400" />
            <Input
              value={schoolQuery}
              onChange={(e) => setSchoolQuery(e.target.value)}
              placeholder="Search schools…"
              className="bg-slate-50 border-slate-200"
            />
          </div>
        ) : null}

        <div className="border rounded-lg overflow-hidden">
          <Table>
            <TableHeader>
              <TableRow className="bg-slate-50">
                <TableHead>{level === "district" ? "District" : level === "block" ? "Block" : "School"}</TableHead>
                <TableHead className="w-[140px] text-right">Action</TableHead>
              </TableRow>
            </TableHeader>
            <TableBody>
              {loading ? (
                <TableRow>
                  <TableCell colSpan={2} className="py-10 text-center text-slate-500">
                    Loading…
                  </TableCell>
                </TableRow>
              ) : null}

              {!loading && level === "district" && districts.map((d) => (
                <TableRow key={d.district_code} className="cursor-pointer hover:bg-slate-50" onClick={() => selectDistrict(d)}>
                  <TableCell className="font-medium">
                    <span className="flex items-center gap-2">
                      <Building2 className="w-4 h-4 text-slate-500" />
                      {d.district_name}
                    </span>
                  </TableCell>
                  <TableCell className="text-right">
                    <Button size="sm" variant="outline">
                      Select
                      <ChevronRight className="w-4 h-4 ml-1" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))}

              {!loading && level === "block" && blocks.map((b) => (
                <TableRow key={b.block_code} className="cursor-pointer hover:bg-slate-50" onClick={() => selectBlock(b)}>
                  <TableCell className="font-medium">
                    <span className="flex items-center gap-2">
                      <Building2 className="w-4 h-4 text-slate-500" />
                      {b.block_name}
                    </span>
                  </TableCell>
                  <TableCell className="text-right">
                    <Button size="sm" variant="outline">
                      Select
                      <ChevronRight className="w-4 h-4 ml-1" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))}

              {!loading && level === "school" && schools.map((s) => (
                <TableRow
                  key={s.udise_code}
                  className={`cursor-pointer hover:bg-slate-50 ${scope.udiseCode === s.udise_code ? "bg-slate-50" : ""}`}
                  onClick={() => selectSchool(s)}
                >
                  <TableCell className="font-medium">
                    <span className="flex items-center gap-2">
                      <GraduationCap className="w-4 h-4 text-slate-500" />
                      <span className="line-clamp-1">{s.school_name}</span>
                    </span>
                    <div className="text-xs text-slate-400 mt-1">UDISE: {s.udise_code}</div>
                  </TableCell>
                  <TableCell className="text-right">
                    <Button
                      size="sm"
                      variant="outline"
                      onClick={(e) => {
                        e.stopPropagation();
                        selectSchool(s);
                        navigate(`/school/${s.udise_code}`);
                        onOpenChange(false);
                      }}
                    >
                      Open
                      <ChevronRight className="w-4 h-4 ml-1" />
                    </Button>
                  </TableCell>
                </TableRow>
              ))}

              {!loading && (
                (level === "district" && districts.length === 0) ||
                (level === "block" && blocks.length === 0) ||
                (level === "school" && schools.length === 0)
              ) ? (
                <TableRow>
                  <TableCell colSpan={2} className="py-10 text-center text-slate-500">
                    No results.
                  </TableCell>
                </TableRow>
              ) : null}
            </TableBody>
          </Table>
        </div>

        <div className="text-xs text-slate-500">
          Tip: selecting a district/block/school updates the global scope, so all dashboards automatically refresh with that filter.
        </div>
      </DialogContent>
    </Dialog>
  );
}


