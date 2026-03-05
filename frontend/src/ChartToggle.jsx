import './ChartToggle.css';

export default function ChartToggle({ view, onChange }) {
  return (
    <div className="chart-toggle">
      <button
        className={`chart-toggle-btn${view === 'table' ? ' active' : ''}`}
        onClick={() => onChange('table')}
        title="Table view"
      >
        Table
      </button>
      <button
        className={`chart-toggle-btn${view === 'chart' ? ' active' : ''}`}
        onClick={() => onChange('chart')}
        title="Chart view"
      >
        Chart
      </button>
    </div>
  );
}
