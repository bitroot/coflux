import { useState, useEffect } from "react";
import { DateTime } from "luxon";

export default function useNow(intervalMs = 1000) {
  const [now, setNow] = useState(DateTime.now());
  useEffect(() => {
    if (intervalMs) {
      const interval = setInterval(() => setNow(DateTime.now()), intervalMs);
      return () => clearInterval(interval);
    }
  }, [intervalMs]);
  return now;
}
