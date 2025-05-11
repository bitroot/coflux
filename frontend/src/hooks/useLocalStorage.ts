import { useState, useEffect, useCallback, useMemo } from "react";

export default function useLocalStorage<T>(
  key: string,
  initialValue: T,
): [T, (value: T | undefined) => void] {
  const [count, setCount] = useState(0);

  const value = useMemo<T>(() => {
    const json = localStorage.getItem(key);
    return json !== null ? JSON.parse(json) : undefined;
  }, [key, count]);

  const update = useCallback(
    (value: T | undefined) => {
      if (value === undefined) {
        localStorage.removeItem(key);
      } else {
        localStorage.setItem(key, JSON.stringify(value));
      }
      window.dispatchEvent(new Event("storage"));
    },
    [key],
  );

  const handleStorageEvent = useCallback(() => {
    setCount((c) => c + 1);
  }, []);

  useEffect(() => {
    window.addEventListener("storage", handleStorageEvent);
    return () => window.removeEventListener("storage", handleStorageEvent);
  }, [handleStorageEvent]);

  return [value !== undefined ? value : initialValue, update];
}
