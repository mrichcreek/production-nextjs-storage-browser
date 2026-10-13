'use client';

// A stored Excel workbook read in the browser: one tab per sheet, a search,
// and the rows a page at a time. The server reads the file and leaves out the
// columns no row uses; whoever opens the viewer supplies `load`, which checks
// the caller's access on the server.
import React, { useEffect, useId, useRef, useState } from 'react';
import { ApiResult } from './symphony';
import './workbook.css';

type Cell = string | number | null;
export interface WorkbookPage extends ApiResult {
  sheets?: { name: string; rows: number }[];
  sheet?: string;
  headers?: string[];
  rows?: Cell[][];
  levels?: [number, boolean][];   // indent and bold of each row's first cell (the Summary's outline)
  hidden_columns?: number;
  total?: number;
  offset?: number;
  limit?: number;
}
export type LoadWorkbookPage = (sheet: string, offset: number, query: string) => Promise<WorkbookPage>;

interface Props {
  title: string;
  load: LoadWorkbookPage;
  onClose: () => void;
  onDownload?: () => void;
}

export const isWorkbook = (name?: string | null) => (name || '').toLowerCase().endsWith('.xlsx');

const n = (v: number) => v.toLocaleString();

export default function WorkbookViewer({ title, load, onClose, onDownload }: Props) {
  const titleId = useId();
  const [sheet, setSheet] = useState('');      // '' = the workbook's first sheet
  const [offset, setOffset] = useState(0);
  const [query, setQuery] = useState('');      // the search applied
  const [typed, setTyped] = useState('');
  const [page, setPage] = useState<WorkbookPage | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState('');
  const latest = useRef(0);
  const closeRef = useRef<HTMLButtonElement>(null);

  useEffect(() => {
    const mine = ++latest.current;
    setLoading(true);
    setError('');
    load(sheet, offset, query).then(d => {
      if (mine !== latest.current) return;
      if (d.ok) setPage(d);
      else setError(d.error || 'The file could not be shown. Please try again in a moment.');
      setLoading(false);
    });
  }, [load, sheet, offset, query]);

  useEffect(() => { closeRef.current?.focus(); }, []);

  const choose = (name: string) => { setSheet(name); setOffset(0); setQuery(''); setTyped(''); };
  const search = (e: React.FormEvent) => { e.preventDefault(); setQuery(typed.trim()); setOffset(0); };

  const active = page?.sheet || '';
  const total = page?.total ?? 0;
  const first = page?.offset ?? 0;
  const size = page?.limit ?? 100;
  const rows = page?.rows ?? [];
  const levels = page?.levels ?? [];

  return (
    <div className="wb-overlay" onMouseDown={e => { if (e.target === e.currentTarget) onClose(); }}
      onKeyDown={e => { if (e.key === 'Escape') onClose(); }}>
      <div className="wb-dialog" role="dialog" aria-modal="true" aria-labelledby={titleId}>
        <div className="wb-head">
          <h2 id={titleId} className="wb-title">{title}</h2>
          <div className="wb-head-actions">
            {onDownload && <button type="button" className="btn btn-primary" onClick={onDownload}>Download</button>}
            <button ref={closeRef} type="button" className="btn btn-secondary" onClick={onClose}>Close</button>
          </div>
        </div>

        {page?.sheets && page.sheets.length > 0 && (
          <div className="wb-tabs" role="tablist" aria-label="Sheets">
            {page.sheets.map(s => (
              <button key={s.name} type="button" role="tab" aria-selected={s.name === active}
                className="wb-tab" onClick={() => choose(s.name)}>
                {s.name} <span className="wb-tab-count">{n(s.rows)}</span>
              </button>
            ))}
          </div>
        )}

        <div className="wb-tools">
          <form className="wb-search" onSubmit={search} role="search">
            <input type="search" className="sy-input" value={typed} onChange={e => setTyped(e.target.value)}
              placeholder="Search this sheet" aria-label="Search this sheet" />
            <button type="submit" className="btn btn-secondary">Search</button>
            {query && <button type="button" className="sy-link" onClick={() => { setTyped(''); setQuery(''); setOffset(0); }}>Clear</button>}
          </form>
          <div className="wb-paging">
            <span className="sy-total" role="status">
              {loading ? 'Loading…' : total === 0 ? (query ? `No row contains “${query}”` : 'No rows')
                : `${n(first + 1)}–${n(first + rows.length)} of ${n(total)}${query ? ` containing “${query}”` : ''}`}
            </span>
            <button type="button" className="btn btn-secondary" disabled={loading || first === 0}
              onClick={() => setOffset(Math.max(0, first - size))}>‹ Previous</button>
            <button type="button" className="btn btn-secondary" disabled={loading || first + size >= total}
              onClick={() => setOffset(first + size)}>Next ›</button>
          </div>
        </div>
        {!!page?.hidden_columns && (
          <p className="wb-hint">{n(page.hidden_columns)} {page.hidden_columns === 1 ? 'column is' : 'columns are'} empty on every row and not shown. The downloaded file has them all.</p>
        )}

        <div className="wb-body">
          {error ? <div className="sy-error wb-message" role="alert">{error}</div>
            : !page ? <p className="sy-muted wb-message">Loading…</p> : (
              <table className={`sy-table wb-table${loading ? ' wb-stale' : ''}`}>
                <thead>
                  <tr>{(page.headers ?? []).map((h, i) => <th key={i}>{h}</th>)}</tr>
                </thead>
                <tbody>
                  {rows.map((r, i) => {
                    const [indent, bold] = levels[i] ?? [0, false];
                    return (
                      <tr key={first + i} className={bold ? 'wb-bold' : undefined}>
                        {r.map((v, j) => (
                          <td key={j} className={typeof v === 'number' ? 'num' : 'wb-cell'}
                            style={j === 0 && indent ? { paddingLeft: 10 + indent * 20 } : undefined}>
                            {v === null ? '' : typeof v === 'number' ? n(v) : v}
                          </td>
                        ))}
                      </tr>
                    );
                  })}
                </tbody>
              </table>
            )}
        </div>
      </div>
    </div>
  );
}
