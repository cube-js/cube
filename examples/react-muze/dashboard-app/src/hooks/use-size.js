import { useState, useLayoutEffect } from "react";
import useResizeObserver from "@react-hook/resize-observer";

const useSize = (target) => {
  const [size, setSize] = useState({ width: undefined, height: undefined });

  useLayoutEffect(
    () => setSize(target.current.getBoundingClientRect()),
    [target]
  );

  useResizeObserver(target, ({ contentRect }) => setSize(contentRect));

  return size;
};

export default useSize;
