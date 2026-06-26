const amqp = require('amqplib');
const mongoose = require('mongoose');
const nodemailer = require('nodemailer');
const NotificationLog = require('./models/NotificationLog');

const MAIN_QUEUE = 'notifications_v3_queue'; 
const DLX_EXCHANGE = 'notification_dlx_v3';
const RETRY_QUEUE = 'retry_queue_v3';

// 1. Database Connection (Cloud MongoDB URL Support with Fallback)
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/nexus_db';
mongoose.connect(MONGO_URI)
  .then(() => console.log('📦 Worker Connected to MongoDB!'))
  .catch((err) => console.error('❌ Worker DB Connection Error:', err));

// 2. Nodemailer Transporter Configuration
const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: 'chitalkarnikita9@gmail.com', 
        pass: 'altt oryu zyqp bwgz'         
    }
});

async function startWorker() {
    try {
        const RABBITMQ_URL = process.env.RABBITMQ_URL || 'amqp://127.0.0.1:5672';
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();
        
        // Match the exact topology parameters from app.js to prevent 406 errors
        await channel.assertExchange(DLX_EXCHANGE, 'direct', { durable: true });
        await channel.assertQueue(MAIN_QUEUE, {
            durable: true,
            arguments: {
                'x-dead-letter-exchange': DLX_EXCHANGE,
                'x-dead-letter-routing-key': RETRY_QUEUE
            }
        });
        
        // Prefetch limit taaki load distributed rahe
        channel.prefetch(1);
        console.log('🚀 Worker is active and listening for messages...');

        // 3. Process Messages From The Queue
        channel.consume(MAIN_QUEUE, async (msg) => {
            if (msg !== null) {
                const messageContent = JSON.parse(msg.content.toString());
                console.log(`\n📥 Received message for processing:`, messageContent);

                const { logId, userId, channel: msgChannel, templateType } = messageContent;

                try {
                    // Safe Case Check: String case change handle karne ke liye (.toUpperCase())
                    if (msgChannel && msgChannel.toUpperCase() === 'EMAIL') {
                        console.log(`📧 Dispatching email to ${userId}...`);

                        const mailOptions = {
                            from: 'chitalkarnikita9@gmail.com',     
                            to: 'nikitachitalkar29@gmail.com',       
                            subject: `NexusNotify - ${templateType}`,
                            text: `Hello ${userId},\n\nYour notification for ${templateType} has been processed successfully via Nexus Distributed System Design!\n\nBest Regards,\nNikita`
                        };

                        const info = await transporter.sendMail(mailOptions);
                        console.log('✨ Email sent successfully! MessageId:', info.messageId);
                    } else {
                        console.log(`ℹ️ Task received for non-email channel [${msgChannel}], logging success internally.`);
                    }

                    // On absolute success, update log and acknowledge message
                    await NotificationLog.findByIdAndUpdate(logId, { status: 'SUCCESS' });
                    console.log(`✅ Successfully processed ${templateType}. DB Status Updated.`);
                    channel.ack(msg);

                } catch (error) {
                    console.error(`❌ Processing Error: ${error.message}`);

                    // 4. Inspect RabbitMQ "x-death" headers to extract the current retry count
                    const deathHeader = msg.properties.headers && msg.properties.headers['x-death'];
                    const retryCount = deathHeader ? deathHeader[0].count : 0;

                    console.log(`⚠️ Current Retry Count: ${retryCount}`);

                    if (retryCount < 3) {
                        console.log(`🔄 Under retry limit. Dropping to Retry Queue for a 5-second cooldown...`);
                        channel.nack(msg, false, false); 
                    } else {
                        console.log(`🚫 Max retries reached. Marking message as permanently FAILED in DB.`);
                        
                        await NotificationLog.findByIdAndUpdate(logId, {
                            status: 'FAILED',
                            errorMessage: `Max retries exhausted. Original error: ${error.message}`
                        });
                        
                        channel.ack(msg); 
                    }
                }
            }
        });

    } catch (error) {
        console.error('❌ Worker RabbitMQ Error:', error);
    }
}

startWorker();