import React, { useState } from 'react';
import axios from 'axios';

function App() {
  const [formData, setFormData] = useState({
    userId: 'Nikita',
    email: 'nikitachitalkar29@gmail.com', // Explicit email field for backend mapping
    channel: 'EMAIL',
    templateType: 'WELCOME'
  });
  const [loading, setLoading] = useState(false);
  const [responseStatus, setResponseStatus] = useState(null);

  // Localhost First, with Production Render Fallback
  const BACKEND_URL = process.env.NODE_ENV === 'production'
    ? "https://nexus-notify.onrender.com/api/v1/notifications/send"
    : "http://localhost:5000/api/v1/notifications/send";

  const handleSubmit = async (e) => {
    e.preventDefault();
    setLoading(true);
    setResponseStatus(null);

    try {
      const response = await axios.post(BACKEND_URL, formData);
      setResponseStatus({
        success: true,
        message: response.data.message || 'Notification queued successfully!',
        logId: response.data.logId
      });
    } catch (error) {
      const serverError = error.response?.data?.details || error.response?.data?.error || error.response?.data?.message || 'Failed to connect to backend API server.';
      
      setResponseStatus({
        success: false,
        message: serverError
      });
    } finally {
      setLoading(false);
    }
  };

  return (
    <div style={{
      backgroundColor: '#0a0a0c',
      color: '#f5f5f5',
      minHeight: '100vh',
      fontFamily: 'sans-serif',
      display: 'flex',
      alignItems: 'center',
      justifyContent: 'center',
      padding: '20px',
      boxSizing: 'border-box'
    }}>
      <div style={{
        background: '#121214',
        border: '1px solid rgba(255, 255, 255, 0.1)',
        borderRadius: '16px',
        padding: '40px',
        width: '100%',
        maxWidth: '450px',
        boxShadow: '0 20px 40px rgba(0,0,0,0.4)',
        textAlign: 'left'
      }}>
        {/* Header */}
        <div style={{ marginBottom: '30px' }}>
          <h2 style={{ margin: 0, fontSize: '24px', fontWeight: '700', color: '#fff' }}>
            Nexus <span style={{ color: '#deff9a' }}>Notify</span>
          </h2>
          <p style={{ margin: '5px 0 0 0', fontSize: '13px', color: '#8a8a93' }}>Control Panel • Active Mode</p>
        </div>

        {/* Form */}
        <form onSubmit={handleSubmit} style={{ display: 'flex', flexDirection: 'column', gap: '20px' }}>
          
          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
            <label style={{ fontSize: '14px', color: '#fff', fontWeight: '500' }}>User ID / Name</label>
            <input 
              type="text" 
              value={formData.userId}
              onChange={(e) => setFormData({ ...formData, userId: e.target.value })}
              placeholder="e.g., Nikita"
              required
              style={{
                background: '#1a1a1e',
                border: '1px solid rgba(255, 255, 255, 0.1)',
                borderRadius: '8px',
                padding: '12px',
                color: '#fff',
                fontSize: '15px',
                outline: 'none'
              }}
            />
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
            <label style={{ fontSize: '14px', color: '#fff', fontWeight: '500' }}>Recipient Email</label>
            <input 
              type="email" 
              value={formData.email}
              onChange={(e) => setFormData({ ...formData, email: e.target.value })}
              placeholder="e.g., nikitachitalkar29@gmail.com"
              required
              style={{
                background: '#1a1a1e',
                border: '1px solid rgba(255, 255, 255, 0.1)',
                borderRadius: '8px',
                padding: '12px',
                color: '#fff',
                fontSize: '15px',
                outline: 'none'
              }}
            />
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
            <label style={{ fontSize: '14px', color: '#fff', fontWeight: '500' }}>Delivery Channel</label>
            <select 
              value={formData.channel}
              onChange={(e) => setFormData({ ...formData, channel: e.target.value })}
              style={{
                background: '#1a1a1e',
                border: '1px solid rgba(255, 255, 255, 0.1)',
                borderRadius: '8px',
                padding: '12px',
                color: '#fff',
                fontSize: '15px',
                cursor: 'pointer',
                outline: 'none'
              }}
            >
              <option value="EMAIL">Email</option>
              <option value="SMS">SMS</option>
              <option value="PUSH">Push</option>
            </select>
          </div>

          <div style={{ display: 'flex', flexDirection: 'column', gap: '8px' }}>
            <label style={{ fontSize: '14px', color: '#fff', fontWeight: '500' }}>Template Type</label>
            <select 
              value={formData.templateType}
              onChange={(e) => setFormData({ ...formData, templateType: e.target.value })}
              style={{
                background: '#1a1a1e',
                border: '1px solid rgba(255, 255, 255, 0.1)',
                borderRadius: '8px',
                padding: '12px',
                color: '#fff',
                fontSize: '15px',
                cursor: 'pointer',
                outline: 'none'
              }}
            >
              <option value="WELCOME">WELCOME_KIT</option>
              <option value="OTP">SECURE_OTP</option>
              <option value="ALERT">SYSTEM_ALERT</option>
            </select>
          </div>

          <button 
            type="submit" 
            disabled={loading}
            style={{
              background: loading ? '#27272a' : '#deff9a',
              color: loading ? '#a1a1aa' : '#000',
              border: 'none',
              borderRadius: '8px',
              padding: '14px',
              fontSize: '16px',
              fontWeight: '600',
              cursor: loading ? 'not-allowed' : 'pointer',
              marginTop: '10px',
              width: '100%'
            }}
          >
            {loading ? 'Connecting Pipeline...' : 'Dispatch Notification'}
          </button>
        </form>

        {/* Status Alerts */}
        {responseStatus && (
          <div style={{
            marginTop: '25px',
            padding: '16px',
            borderRadius: '8px',
            background: responseStatus.success ? 'rgba(34, 197, 94, 0.1)' : 'rgba(239, 68, 68, 0.1)',
            border: responseStatus.success ? '1px solid rgba(34, 197, 94, 0.2)' : '1px solid rgba(239, 68, 68, 0.2)',
          }}>
            <p style={{ margin: 0, fontSize: '14px', fontWeight: '600', color: responseStatus.success ? '#22c55e' : '#ef4444' }}>
              {responseStatus.success ? ' Request Accepted' : 'Pipeline Status'}
            </p>
            <p style={{ margin: '4px 0 0 0', fontSize: '13px', color: '#e4e4e7', fontWeight: '500' }}>
              {responseStatus.message}
            </p>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;