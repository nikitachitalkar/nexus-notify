const mongoose = require('mongoose');

const notificationLogSchema = new mongoose.Schema({
  userId: { type: String, required: true },
  channel: { type: String, required: true, enum: ['EMAIL', 'SMS'] },
  templateType: { type: String, required: true },
  status: { type: String, enum: ['PENDING', 'SENT', 'FAILED'], default: 'PENDING' },
  errorMessage: { type: String, default: null },
  createdAt: { type: Date, default: Date.now }
});

module.exports = mongoose.model('NotificationLog', notificationLogSchema);