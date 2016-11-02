package com.angejia.dw.util;

import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Properties;
import javax.activation.DataHandler;
import javax.activation.FileDataSource;
import javax.mail.BodyPart;
import javax.mail.Message.RecipientType;
import javax.mail.MessagingException;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeBodyPart;
import javax.mail.internet.MimeMessage;
import javax.mail.internet.MimeMultipart;

import org.apache.commons.mail.EmailException;
import org.apache.commons.mail.HtmlEmail;

public class MailSenderUtil
{   
    public static void sendSmtp(String subject, String smtpHost, String content, String[] filePaths, String username,
            String password, String to, String from) throws MessagingException
    {
        Properties props = new Properties();
        props.put("mail.smtp.host", smtpHost);
        props.put("mail.needAuth", Boolean.valueOf(true));
        
        Session session = Session.getDefaultInstance(props);
        MimeMessage msg = new MimeMessage(session);
        
        InternetAddress address = new InternetAddress(from);
        //发送者
        msg.setSender(address);
        //主题
        msg.setSubject(subject, "utf-8");
        //发件人
        msg.setFrom(address);
        //收件人
        InternetAddress[] toAddress = InternetAddress.parse(to);
        msg.setReplyTo(toAddress);
        msg.setRecipients(RecipientType.TO, toAddress);

        MimeMultipart mmp = new MimeMultipart();
        if (filePaths != null)
        {
            for (String file : filePaths)
            {
                BodyPart fp = new MimeBodyPart();
                FileDataSource fileds = new FileDataSource(file);
                fp.setDataHandler(new DataHandler(fileds));
                fp.setFileName(fileds.getName());
                mmp.addBodyPart(fp);
            }
        }

        BodyPart bodyPart = new MimeBodyPart();
        bodyPart.setContent("<meta http-equiv=Content-Type content=text/html; charset=utf-8>" + content,
                "text/html;charset=utf-8");
        mmp.addBodyPart(bodyPart);

        msg.setContent(mmp);
        msg.saveChanges();

        Transport transPort = session.getTransport("smtp");
        transPort.connect(props.getProperty("mail.smtp.host"), username, password);
        transPort.sendMessage(msg, msg.getRecipients(RecipientType.TO));
        transPort.close();
    }
    
    public static void sendHTMLMailFromDWMS(String title, List<String> receivers, String content) 
        throws EmailException, UnsupportedEncodingException
    {
        HtmlEmail  email = new HtmlEmail();
        email.setHostName("smtp.angejia.com");
        email.setAuthentication( "dl-bi@angejia.com", "");
        email.setFrom("dwms@angejia.com");
        email.setSubject(title);
        email.setMsg(content);
  
        for(String receiver : receivers)
            email.addTo(receiver);
        
        email.send();
    }
}