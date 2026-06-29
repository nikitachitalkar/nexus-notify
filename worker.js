const amqp = require('amqplib');
const mongoose = require('mongoose');
const nodemailer = require('nodemailer');
const NotificationLog = require('./models/NotificationLog');

const MAIN_QUEUE = 'notifications_v3_queue'; 
const DLX_EXCHANGE = 'notification_dlx_v3';
const RETRY_QUEUE = 'retry_queue_v3';

// 1. Database Connection (Cloud MongoDB URL Support)
const MONGO_URI = process.env.MONGO_URI || 'mongodb://localhost:27017/nexus_db';
mongoose.connect(MONGO_URI)
  .then(() => console.log('📦 Worker Connected to MongoDB!'))
  .catch((err) => console.error('❌ Worker DB Connection Error:', err));

// 2. Nodemailer Transporter Configuration
const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
        user: 'chitalkarnikita9@gmail.com', 
        pass: 'vsie trhr fffo dpmo' // Verfied Gmail App Password        
    }
});

async function startWorker() {
    try {
        // 🔥 Live Verified CloudAMQP Broker Connection URL
        const RABBITMQ_URL = 'amqps://azteeckf:I9UvXzG1LH83FZG-aD51_OCxgRupLvTJ@seal.lmq.cloudamqp.com/azteeckf';
        
        console.log('⏳ Connecting local worker to CloudAMQP Broker...');
        const connection = await amqp.connect(RABBITMQ_URL);
        const channel = await connection.createChannel();
        
        // Asserting exact same topology configurations
        await channel.assertExchange(DLX_EXCHANGE, 'direct', { durable: true });
        await channel.assertQueue(MAIN_QUEUE, {
            durable: true,
            arguments: {
                'x-dead-letter-exchange': DLX_EXCHANGE,
                'x-dead-letter-routing-key': RETRY_QUEUE
            }
        });
        
        channel.prefetch(1);
        console.log('🚀 Worker is active and listening directly to CloudAMQP!');

        // 3. Process Messages From The Queue
        channel.consume(MAIN_QUEUE, async (msg) => {
            if (msg !== null) {
                const messageContent = JSON.parse(msg.content.toString());
                console.log(`\n📥 Received message for processing:`, messageContent);

                const { logId, userId, channel: msgChannel, templateType } = messageContent;

                try {
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

                    // Acknowledge message on absolute success
                    try {
                        await NotificationLog.findByIdAndUpdate(logId, { status: 'SUCCESS' });
                        console.log(`✅ Successfully processed ${templateType}. DB Status Updated.`);
                    } catch (dbErr) {
                        console.log(`⚠️ DB Log update bypassed: ${dbErr.message}`);
                    }
                    channel.ack(msg);

                } catch (error) {
                    console.error(`❌ Processing Error inside Nodemailer: ${error.message}`);

                    const deathHeader = msg.properties.headers && msg.properties.headers['x-death'];
                    const retryCount = deathHeader ? deathHeader[0].count : 0;

                    console.log(`⚠️ Current Retry Count: ${retryCount}`);

                    if (retryCount < 3) {
                        console.log(`🔄 Under retry limit. Dropping to Retry Queue for a 5-second cooldown...`);
                        channel.nack(msg, false, false); 
                    } else {
                        console.log(`🚫 Max retries reached. Marking message as FAILED.`);
                        try {
                            await NotificationLog.findByIdAndUpdate(logId, {
                                status: 'FAILED',
                                errorMessage: `Max retries exhausted. Original error: ${error.message}`
                            });
                        } catch (dbErr) {}
                        channel.ack(msg); 
                    }
                }
            }
        });

    } catch (error) {
        console.error('❌ Worker RabbitMQ Connection Error:', error.message);
    }
}

startWorker();