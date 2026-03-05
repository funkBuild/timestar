import { useEffect, useRef } from 'react';
import uPlot from 'uplot';
import 'uplot/dist/uPlot.min.css';

// UPlotChart renders a uPlot chart that fills the width of its container.
// props:
//   data  - uPlot data array: [timestamps_seconds, series1_values, ...]
//   opts  - uPlot opts object (width will be overridden with container width)
//   style - optional extra styles for the wrapper div
export default function UPlotChart({ data, opts, style }) {
  const wrapRef = useRef(null);
  const plotRef = useRef(null);

  useEffect(() => {
    if (!wrapRef.current || !data || data.length === 0) return;

    // Destroy any existing instance first
    if (plotRef.current) {
      plotRef.current.destroy();
      plotRef.current = null;
    }

    const width = wrapRef.current.offsetWidth || 700;
    const mergedOpts = { ...opts, width };

    plotRef.current = new uPlot(mergedOpts, data, wrapRef.current);

    // Resize when container changes size
    const ro = new ResizeObserver((entries) => {
      const entry = entries[0];
      if (entry && plotRef.current) {
        const newWidth = entry.contentRect.width;
        if (newWidth > 0) {
          plotRef.current.setSize({ width: newWidth, height: mergedOpts.height || 300 });
        }
      }
    });
    ro.observe(wrapRef.current);

    return () => {
      ro.disconnect();
      if (plotRef.current) {
        plotRef.current.destroy();
        plotRef.current = null;
      }
    };
  }, [data, opts]);

  return <div ref={wrapRef} style={style} />;
}
