import React, { createContext, useCallback, useContext, useMemo, useState } from "react";

const STORAGE_KEY = "dashboard_scope_v1";

function loadInitialScope() {
  try {
    const raw = localStorage.getItem(STORAGE_KEY);
    if (!raw) return null;
    const parsed = JSON.parse(raw);
    if (!parsed || typeof parsed !== "object") return null;
    return {
      districtCode: parsed.districtCode || "",
      blockCode: parsed.blockCode || "",
      udiseCode: parsed.udiseCode || "",
      districtName: parsed.districtName || "",
      blockName: parsed.blockName || "",
      schoolName: parsed.schoolName || "",
    };
  } catch {
    return null;
  }
}

function persistScope(scope) {
  try {
    localStorage.setItem(
      STORAGE_KEY,
      JSON.stringify({
        districtCode: scope.districtCode || "",
        blockCode: scope.blockCode || "",
        udiseCode: scope.udiseCode || "",
        districtName: scope.districtName || "",
        blockName: scope.blockName || "",
        schoolName: scope.schoolName || "",
      }),
    );
  } catch {
    // ignore storage errors (private mode, quota, etc.)
  }
}

const ScopeContext = createContext(null);

export const ScopeProvider = ({ children }) => {
  const initial = loadInitialScope();
  const [scope, setScopeState] = useState(() => ({
    districtCode: initial?.districtCode || "",
    blockCode: initial?.blockCode || "",
    udiseCode: initial?.udiseCode || "",
    districtName: initial?.districtName || "",
    blockName: initial?.blockName || "",
    schoolName: initial?.schoolName || "",
    version: 0,
  }));

  const setScope = useCallback((next) => {
    setScopeState((prev) => {
      const merged = {
        ...prev,
        ...next,
      };

      // Cascade resets
      if (next?.districtCode !== undefined && next.districtCode !== prev.districtCode) {
        merged.blockCode = "";
        merged.blockName = "";
        merged.udiseCode = "";
        merged.schoolName = "";
      }
      if (next?.blockCode !== undefined && next.blockCode !== prev.blockCode) {
        merged.udiseCode = "";
        merged.schoolName = "";
      }

      const updated = { ...merged, version: (prev.version || 0) + 1 };
      persistScope(updated);
      return updated;
    });
  }, []);

  const clearScope = useCallback(() => {
    setScope({
      districtCode: "",
      districtName: "",
      blockCode: "",
      blockName: "",
      udiseCode: "",
      schoolName: "",
    });
  }, [setScope]);

  const value = useMemo(() => ({ scope, setScope, clearScope }), [scope, setScope, clearScope]);

  return <ScopeContext.Provider value={value}>{children}</ScopeContext.Provider>;
};

export const useScope = () => {
  const ctx = useContext(ScopeContext);
  if (!ctx) throw new Error("useScope must be used within a ScopeProvider");
  return ctx;
};


