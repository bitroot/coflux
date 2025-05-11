import { useEffect, useState } from "react";

type Props = {
  delayMs?: number;
};

export default function Loading({ delayMs }: Props) {
  const [show, setShow] = useState(false);
  useEffect(() => {
    const timeout = setTimeout(() => {
      setShow(true);
    }, delayMs ?? 1000);
    return () => clearTimeout(timeout);
  }, [delayMs]);
  if (show) {
    return <p>Loading...</p>;
  } else {
    return null;
  }
}
