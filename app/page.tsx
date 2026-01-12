'use client';
import React, { useEffect, useState } from 'react';
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

// Folder configuration with icons and colors
const folders = [
  { path: 'ConversionFiles/', name: 'Conversion Files', icon: '📄', type: 'conversion' },
  { path: 'ConversionFileErrors/', name: 'Conversion Errors', icon: '⚠️', type: 'error' },
  { path: 'InitialUpload/', name: 'Initial Upload', icon: '📤', type: 'upload' },
  { path: 'InitialUploadErrors/', name: 'Upload Errors', icon: '❌', type: 'error' },
  { path: 'TSQLFiles/', name: 'TSQL Files', icon: '🗃️', type: 'sql' },
  { path: 'DataValidation/', name: 'Data Validation', icon: '✅', type: 'validation' },
];

function FileBrowser() {
  const [userEmail, setUserEmail] = useState<string>('');
  const [currentPath, setCurrentPath] = useState<string>('');
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [storageKey, setStorageKey] = useState(0);

  useEffect(() => {
    // Check for navigation target in sessionStorage
    const targetPath = sessionStorage.getItem('s3NavigationTarget');
    if (targetPath) {
      setCurrentPath(targetPath);
      sessionStorage.removeItem('s3NavigationTarget');
    }

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

  // Create storage browser with current configuration
  const { StorageBrowser } = createStorageBrowser({
    elements: elementsDefault,
    config: createAmplifyAuthAdapter({
      options: {
        defaultPrefixes: folders.map(f => f.path),
        ...(currentPath && { initialPath: currentPath }),
      },
    }),
  });

  // Handle folder navigation
  const handleFolderClick = (path: string) => {
    setCurrentPath(path);
    setStorageKey(prev => prev + 1);
    setSidebarOpen(false);
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
    return folder ? folder.name : currentPath.replace('/', '');
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
            style={{ display: 'none' }}
          >
            <span style={{ fontSize: '20px' }}>☰</span>
          </button>

          <div className="header-logo">H</div>
          <h1 className="header-title">Hacienda ERP File Browser</h1>
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
        <aside className={`sidebar ${sidebarOpen ? 'open' : ''}`}>
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
        </aside>

        {/* Main Content */}
        <main className="main-content">
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
              </>
            )}
          </div>

          {/* Storage Browser */}
          <div className="storage-browser-wrapper">
            <StorageBrowser key={storageKey} />
          </div>
        </main>
      </div>

      {/* Mobile menu toggle styles - inline for visibility control */}
      <style jsx>{`
        @media (max-width: 768px) {
          .menu-toggle {
            display: flex !important;
          }
        }
      `}</style>
    </div>
  );
}

export default withAuthenticator(FileBrowser);
