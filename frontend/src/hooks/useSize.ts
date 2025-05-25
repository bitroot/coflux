import { useState, useLayoutEffect } from "react";

type Size = {
  width: number;
  height: number;
};

export default function useSize<T extends HTMLElement>(): [
  (node: T | null) => void,
  Size,
] {
  const [node, setNode] = useState<T | null>(null);
  const [size, setSize] = useState<Size>({ width: 0, height: 0 });

  useLayoutEffect(() => {
    if (!node) return;

    const rect = node.getBoundingClientRect();
    setSize({ width: rect.width, height: rect.height });

    const observer = new ResizeObserver((entries) => {
      for (const entry of entries) {
        const { width, height } = entry.contentRect;
        setSize({ width, height });
      }
    });
    observer.observe(node);

    return () => {
      observer.disconnect();
    };
  }, [node]);

  return [setNode, size];
}
