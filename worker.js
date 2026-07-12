require('dotenv').config();
const amqp = require('amqplib');
const mongoose = require('mongoose');
const nodemailer = require('nodemailer');
const NotificationLog = require('./models/NotificationLog');

// Queue & Exchange Config
const MAIN_QUEUE = 'notifications_v5_queue'; 
const DLX_EXCHANGE = 'notification_dlx_v5';
const DLQ_FINAL = 'dead_letter_queue_v5';

const MONGO_URI = process.env.MONGO_URI || 'mongodb://127.0.0.1:27017/nexus_db';
if (mongoose.connection.readyState === 0) {
    mongoose.connect(MONGO_URI)
      .then(() => console.log('📦 Worker Connected to MongoDB!'))
      .catch(() => console.log('ℹ️ Worker running in DB-bypass mode'));
}

// Nodemailer Transporter Setup
const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: process.env.EMAIL_USER, 
        pass: process.env.EMAIL_PASS       
    }
});

async function startWorker() {
    try {
        const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://localhost';
        
        console.log('⏳ Connecting worker to CloudAMQP Broker...');
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();

        // 1. Setup DLX (Dead Letter Exchange)
        await channel.assertExchange(DLX_EXCHANGE, 'direct', { durable: true });

        // 2. Setup Final DLQ
        await channel.assertQueue(DLQ_FINAL, { durable: true });

        // 3. Setup Main Notification Queue linked with DLX
        await channel.assertQueue(MAIN_QUEUE, {
            durable: true,
            arguments: {
                'x-dead-letter-exchange': DLX_EXCHANGE,
                'x-dead-letter-routing-key': DLQ_FINAL
            }
        });

        channel.prefetch(1);
        console.log('🚀 Resilient Worker with DLQ Active & Listening!');

        channel.consume(MAIN_QUEUE, async (msg) => {
            if (msg !== null) {
                const messageContent = JSON.parse(msg.content.toString());
                console.log(`\n📥 [MESSAGE RECEIVED]:`, messageContent);

                const { logId, userId, templateType, email } = messageContent;

                try {
                    // Simulate Failure Test Check
                    if (email === 'fail@test.com') {
                        throw new Error("Simulated Bad Request / Invalid Recipient");
                    }

                    const recipientEmail = email || 'nikitachitalkar29@gmail.com';
                    console.log(`📧 Attempting Nodemailer dispatch to: ${recipientEmail}...`);

                    const mailOptions = {
                        from: `Nexus Notify <${process.env.EMAIL_USER}>`,     
                        to: recipientEmail,       
                        subject: `NexusNotify Alert - ${templateType}`,
                        text: `Hello ${userId},\n\nYour notification for [${templateType}] was processed successfully.\n\nBest Regards,\nNexus Architecture`
                    };

                    const info = await transporter.sendMail(mailOptions);
                    console.log('✨ SUCCESS! Email sent via Gmail. ID:', info.messageId);

                    if (logId) {
                        try {
                            await NotificationLog.findByIdAndUpdate(logId, { status: 'SUCCESS' });
                        } catch (dbErr) {}
                    }
                    channel.ack(msg); // Acknowledge success

                } catch (error) {
                    console.error(`❌ DISPATCH FAILED: ${error.message}`);
                    console.log(`⚠️ Moving message to Dead Letter Queue (DLQ)...`);
                    
                    if (logId) {
                        try {
                            await NotificationLog.findByIdAndUpdate(logId, { status: 'FAILED_DLQ' });
                        } catch (dbErr) {}
                    }

                    // Reject message without requeue -> CloudAMQP routes it directly to DLQ
                    channel.nack(msg, false, false);
                }
            }
        });

    } catch (error) {
        console.error('❌ Worker RabbitMQ Error:', error.message);
    }
}

startWorker();