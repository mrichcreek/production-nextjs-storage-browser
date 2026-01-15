'use client';
import React, { useEffect, useState, useMemo } from 'react';
import { Amplify } from 'aws-amplify';
import { signOut, fetchUserAttributes } from 'aws-amplify/auth';
import { Button, withAuthenticator } from '@aws-amplify/ui-react';
import {
  createStorageBrowser,
  createAmplifyAuthAdapter,
  elementsDefault,
} from '@aws-amplify/ui-react-storage/browser';
import '@aws-amplify/ui-react-storage/styles.css';
import '@aws-amplify/ui-react-storage/storage-browser-styles.css';
import config from '../amplify_outputs.json';

Amplify.configure(config);

// Determine environment from bucket name
const getEnvironmentName = () => {
  const bucketName = config.storage?.bucket_name || '';
  if (bucketName.includes('-prd') || bucketName.includes('-prod')) {
    return 'Production';
  } else if (bucketName.includes('-dev')) {
    return 'Development';
  }
  return '';
};

// Type for quick links
interface QuickLink {
  id: string;
  path: string;
  name: string;
}

// Folder configuration with icons and colors
const folders = [
  { path: 'ConversionFiles/', name: 'Conversion Files', icon: '📄', type: 'conversion' },
  { path: 'ConversionFileErrors/', name: 'Conversion Errors', icon: '⚠️', type: 'error' },
  { path: 'InitialUpload/', name: 'Initial Upload', icon: '📤', type: 'upload' },
  { path: 'InitialUploadErrors/', name: 'Upload Errors', icon: '❌', type: 'error' },
  { path: 'TSQLFiles/', name: 'TSQL Files', icon: '🗃️', type: 'sql' },
  { path: 'DataValidation/', name: 'Data Validation', icon: '✅', type: 'validation' },
];

// Default quick links
const defaultQuickLinks: QuickLink[] = [
  { id: 'default-1', path: 'ConversionFileErrors/Mock8/', name: 'Mock8 Errors' },
];

function FileBrowser() {
  const [userEmail, setUserEmail] = useState<string>('');
  const [currentPath, setCurrentPath] = useState<string>('');
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [sidebarCollapsed, setSidebarCollapsed] = useState(false);
  const [refreshKey, setRefreshKey] = useState(0);
  const environmentName = getEnvironmentName();

  // Quick Links state
  const [quickLinks, setQuickLinks] = useState<QuickLink[]>(defaultQuickLinks);
  const [showAddModal, setShowAddModal] = useState(false);
  const [newLinkPath, setNewLinkPath] = useState('');
  const [newLinkName, setNewLinkName] = useState('');
  const [showContextMenu, setShowContextMenu] = useState(false);

  // Load quick links from localStorage on mount
  useEffect(() => {
    const savedLinks = localStorage.getItem('hacienda-quick-links');
    if (savedLinks) {
      try {
        setQuickLinks(JSON.parse(savedLinks));
      } catch (e) {
        console.error('Error loading quick links:', e);
      }
    }
  }, []);

  // Save quick links to localStorage whenever they change
  useEffect(() => {
    localStorage.setItem('hacienda-quick-links', JSON.stringify(quickLinks));
  }, [quickLinks]);

  useEffect(() => {
    // Fetch user attributes
    async function getAttributes() {
      try {
        const attributes = await fetchUserAttributes();
        setUserEmail(attributes.email || '');
      } catch (error) {
        console.error('Error fetching user attributes', error);
      }
    }
    getAttributes();
  }, []);

  // Create storage browser - memoized to prevent unnecessary recreation
  const { StorageBrowser } = useMemo(() => {
    const prefixes = currentPath
      ? [currentPath]
      : folders.map(f => f.path);

    return createStorageBrowser({
      elements: elementsDefault,
      config: createAmplifyAuthAdapter({
        options: {
          defaultPrefixes: prefixes,
        },
      }),
    });
  }, [currentPath, refreshKey]);

  // Handle folder navigation
  const handleFolderClick = (path: string) => {
    setCurrentPath(path);
    setRefreshKey(prev => prev + 1);
    setSidebarOpen(false);
    setShowContextMenu(false);
  };

  // Get user initials for avatar
  const getUserInitials = (email: string) => {
    if (!email) return 'U';
    const parts = email.split('@')[0].split(/[._-]/);
    if (parts.length >= 2) {
      return (parts[0][0] + parts[1][0]).toUpperCase();
    }
    return email.substring(0, 2).toUpperCase();
  };

  // Get current folder name for breadcrumb
  const getCurrentFolderName = () => {
    if (!currentPath) return 'All Folders';
    const folder = folders.find(f => f.path === currentPath);
    if (folder) return folder.name;
    // Check quick links
    const quickLink = quickLinks.find(ql => ql.path === currentPath);
    if (quickLink) return quickLink.name;
    return currentPath.replace(/\/$/, '').split('/').pop() || currentPath;
  };

  // Add a new quick link
  const handleAddQuickLink = () => {
    if (!newLinkPath.trim()) return;

    const path = newLinkPath.trim().endsWith('/') ? newLinkPath.trim() : newLinkPath.trim() + '/';
    const name = newLinkName.trim() || path.replace(/\/$/, '').split('/').pop() || path;

    const newLink: QuickLink = {
      id: `custom-${Date.now()}`,
      path,
      name,
    };

    setQuickLinks(prev => [...prev, newLink]);
    setNewLinkPath('');
    setNewLinkName('');
    setShowAddModal(false);
  };

  // Add current folder as quick link
  const handleAddCurrentAsQuickLink = () => {
    if (!currentPath) return;

    const existingLink = quickLinks.find(ql => ql.path === currentPath);
    if (existingLink) {
      alert('This folder is already in your quick links.');
      setShowContextMenu(false);
      return;
    }

    const name = getCurrentFolderName();
    const newLink: QuickLink = {
      id: `custom-${Date.now()}`,
      path: currentPath,
      name,
    };

    setQuickLinks(prev => [...prev, newLink]);
    setShowContextMenu(false);
  };

  // Delete a quick link
  const handleDeleteQuickLink = (id: string) => {
    setQuickLinks(prev => prev.filter(ql => ql.id !== id));
  };

  return (
    <div className="app-container">
      {/* Header */}
      <header className="app-header">
        <div className="header-left">
          {/* Mobile menu toggle */}
          <button
            className="menu-toggle"
            onClick={() => setSidebarOpen(!sidebarOpen)}
            aria-label="Toggle menu"
          >
            <span>☰</span>
          </button>

          <div className="header-logo">H</div>
          <h1 className="header-title">
            Hacienda ERP File Browser
            {environmentName && <span className={`env-badge env-${environmentName.toLowerCase()}`}>{environmentName}</span>}
          </h1>
        </div>

        <div className="header-right">
          {userEmail && (
            <div className="user-info">
              <div className="user-avatar">{getUserInitials(userEmail)}</div>
              <span className="user-email">{userEmail}</span>
            </div>
          )}
          <Button
            className="sign-out-btn"
            size="small"
            onClick={() => signOut()}
          >
            Sign Out
          </Button>
        </div>
      </header>

      <div className="app-body">
        {/* Mobile overlay */}
        <div
          className={`sidebar-overlay ${sidebarOpen ? 'visible' : ''}`}
          onClick={() => setSidebarOpen(false)}
        />

        {/* Sidebar */}
        <aside className={`sidebar ${sidebarOpen ? 'open' : ''} ${sidebarCollapsed ? 'collapsed' : ''}`}>
          {/* Collapse Toggle Button */}
          <button
            className="sidebar-collapse-btn"
            onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
            title={sidebarCollapsed ? 'Expand sidebar' : 'Collapse sidebar'}
          >
            {sidebarCollapsed ? '»' : '«'}
          </button>

          {/* Folders Section */}
          <h2 className="sidebar-title">Folders</h2>
          <nav>
            <ul className="sidebar-nav">
              <li className="sidebar-item">
                <button
                  className={`sidebar-link ${!currentPath ? 'active' : ''}`}
                  onClick={() => handleFolderClick('')}
                  data-folder="default"
                >
                  <span className="sidebar-icon">🏠</span>
                  <span>All Folders</span>
                </button>
              </li>
              {folders.map((folder) => (
                <li key={folder.path} className="sidebar-item">
                  <button
                    className={`sidebar-link ${currentPath === folder.path ? 'active' : ''}`}
                    onClick={() => handleFolderClick(folder.path)}
                    data-folder={folder.type}
                  >
                    <span className="sidebar-icon">{folder.icon}</span>
                    <span>{folder.name}</span>
                  </button>
                </li>
              ))}
            </ul>
          </nav>

          {/* Quick Links Section */}
          <h2 className="sidebar-title quick-links-title">Quick Links</h2>
          <nav>
            <ul className="sidebar-nav">
              {quickLinks.map((link) => (
                <li key={link.id} className="sidebar-item quick-link-item">
                  <button
                    className={`sidebar-link ${currentPath === link.path ? 'active' : ''}`}
                    onClick={() => handleFolderClick(link.path)}
                    data-folder="quicklink"
                  >
                    <span className="sidebar-icon">⭐</span>
                    <span>{link.name}</span>
                  </button>
                  <button
                    className="quick-link-delete"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDeleteQuickLink(link.id);
                    }}
                    title="Remove quick link"
                  >
                    ×
                  </button>
                </li>
              ))}
              <li className="sidebar-item">
                <button
                  className="sidebar-link add-quick-link"
                  onClick={() => setShowAddModal(true)}
                >
                  <span className="sidebar-icon">➕</span>
                  <span>Add Quick Link</span>
                </button>
              </li>
            </ul>
          </nav>
        </aside>

        {/* Main Content */}
        <main className={`main-content ${sidebarCollapsed ? 'sidebar-collapsed' : ''}`}>
          {/* Breadcrumb */}
          <div className="breadcrumb">
            <span className="breadcrumb-item">
              <button
                className="breadcrumb-link"
                onClick={() => handleFolderClick('')}
                style={{
                  background: 'none',
                  border: 'none',
                  cursor: 'pointer',
                  padding: 0,
                  font: 'inherit'
                }}
              >
                Home
              </button>
            </span>
            {currentPath && (
              <>
                <span className="breadcrumb-separator">/</span>
                <span className="breadcrumb-current">{getCurrentFolderName()}</span>
                {/* Context menu button */}
                <button
                  className="breadcrumb-menu-btn"
                  onClick={() => setShowContextMenu(!showContextMenu)}
                  title="More options"
                >
                  ⋮
                </button>
                {showContextMenu && (
                  <div className="context-menu">
                    <button
                      className="context-menu-item"
                      onClick={handleAddCurrentAsQuickLink}
                    >
                      <span>⭐</span>
                      <span>Add to Quick Links</span>
                    </button>
                  </div>
                )}
              </>
            )}
          </div>

          {/* Storage Browser */}
          <div className="storage-browser-wrapper">
            <StorageBrowser key={refreshKey} />
          </div>
        </main>
      </div>

      {/* Add Quick Link Modal */}
      {showAddModal && (
        <div className="modal-overlay" onClick={() => setShowAddModal(false)}>
          <div className="modal" onClick={(e) => e.stopPropagation()}>
            <div className="modal-header">
              <h3>Add Quick Link</h3>
              <button
                className="modal-close"
                onClick={() => setShowAddModal(false)}
              >
                ×
              </button>
            </div>
            <div className="modal-body">
              <div className="form-group">
                <label htmlFor="linkPath">Folder Path</label>
                <input
                  id="linkPath"
                  type="text"
                  placeholder="e.g., ConversionFiles/Reports/"
                  value={newLinkPath}
                  onChange={(e) => setNewLinkPath(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && handleAddQuickLink()}
                />
              </div>
              <div className="form-group">
                <label htmlFor="linkName">Display Name (optional)</label>
                <input
                  id="linkName"
                  type="text"
                  placeholder="e.g., Reports"
                  value={newLinkName}
                  onChange={(e) => setNewLinkName(e.target.value)}
                  onKeyDown={(e) => e.key === 'Enter' && handleAddQuickLink()}
                />
              </div>
            </div>
            <div className="modal-footer">
              <button
                className="btn btn-secondary"
                onClick={() => setShowAddModal(false)}
              >
                Cancel
              </button>
              <button
                className="btn btn-primary"
                onClick={handleAddQuickLink}
                disabled={!newLinkPath.trim()}
              >
                Add Quick Link
              </button>
            </div>
          </div>
        </div>
      )}

      {/* Click outside to close context menu */}
      {showContextMenu && (
        <div
          className="context-menu-overlay"
          onClick={() => setShowContextMenu(false)}
        />
      )}
    </div>
  );
}

export default withAuthenticator(FileBrowser);
