'use client';

// Asked before a validation workbook is downloaded: as it is, or protected by
// a password the user chooses. The protected copy is made on the server with
// Excel's own encryption, so Excel asks for the password whenever it is opened.
import React, { useEffect, useId, useRef, useState } from 'react';
import './workbook.css';

export const PASSWORD_MIN = 8;
const PASSWORD_MAX = 255;

interface Props {
  fileName: string;
  onClose: () => void;
  /** Start the download (password null = as it is); resolves to an error message when it fails. */
  download: (password: string | null) => Promise<string | void>;
}

export default function DownloadDialog({ fileName, onClose, download }: Props) {
  const titleId = useId();
  const [protect, setProtect] = useState(false);
  const [password, setPassword] = useState('');
  const [confirm, setConfirm] = useState('');
  const [show, setShow] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState('');
  const firstRef = useRef<HTMLInputElement>(null);

  useEffect(() => { firstRef.current?.focus(); }, []);

  const problem = !protect ? ''
    : password.length < PASSWORD_MIN ? `The password needs at least ${PASSWORD_MIN} characters.`
      : password.length > PASSWORD_MAX ? `The password can have at most ${PASSWORD_MAX} characters.`
        : password !== confirm ? 'The two passwords are not the same.' : '';

  const submit = async (e: React.FormEvent) => {
    e.preventDefault();
    if (problem) { setError(problem); return; }
    setBusy(true);
    setError('');
    const failed = await download(protect ? password : null);
    setBusy(false);
    if (failed) setError(failed);
    else onClose();
  };

  const type = show ? 'text' : 'password';
  return (
    <div className="wb-overlay wb-overlay-top" onMouseDown={e => { if (e.target === e.currentTarget && !busy) onClose(); }}
      onKeyDown={e => { if (e.key === 'Escape' && !busy) onClose(); }}>
      <form className="wb-dialog wb-dialog-small" role="dialog" aria-modal="true" aria-labelledby={titleId} onSubmit={submit}>
        <div className="wb-head">
          <h2 id={titleId} className="wb-title">Download the file</h2>
        </div>
        <div className="wb-form">
          <p className="wb-file">{fileName}</p>
          <fieldset className="wb-choice">
            <legend>Do you want to protect the file with a password?</legend>
            <label>
              <input ref={firstRef} type="radio" name="protect" checked={!protect} onChange={() => { setProtect(false); setError(''); }} />
              No, download it as it is
            </label>
            <label>
              <input type="radio" name="protect" checked={protect} onChange={() => { setProtect(true); setError(''); }} />
              Yes, protect it with a password
            </label>
          </fieldset>
          {protect && (
            <>
              <label className="sy-field">
                <span>Password</span>
                <input type={type} value={password} onChange={e => setPassword(e.target.value)} autoComplete="new-password"
                  maxLength={PASSWORD_MAX} aria-describedby={`${titleId}-rule`} autoFocus />
              </label>
              <label className="sy-field">
                <span>Type the password again</span>
                <input type={type} value={confirm} onChange={e => setConfirm(e.target.value)} autoComplete="new-password"
                  maxLength={PASSWORD_MAX} />
              </label>
              <label className="wb-show">
                <input type="checkbox" checked={show} onChange={e => setShow(e.target.checked)} /> Show the password
              </label>
              <p id={`${titleId}-rule`} className="wb-hint wb-hint-tight">
                At least {PASSWORD_MIN} characters. Excel will ask for this password every time the file is opened.
                Keep it safe and share it separately from the file: it cannot be recovered.
              </p>
            </>
          )}
          {error && <div className="sy-error wb-message-tight" role="alert">{error}</div>}
        </div>
        <div className="wb-actions">
          <button type="button" className="btn btn-secondary" onClick={onClose} disabled={busy}>Cancel</button>
          <button type="submit" className="btn btn-primary" disabled={busy}>
            {busy ? (protect ? 'Protecting the file…' : 'Preparing…') : 'Download'}
          </button>
        </div>
      </form>
    </div>
  );
}
