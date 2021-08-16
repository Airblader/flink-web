---
layout: post
title:  "Implementing a custom source connector for Table API and SQL - Part Two "
date: 2021-08-18T00:00:00.000Z
authors:
- Ingo Buerk:
  name: "Ingo Buerk"
excerpt: 
---

{% toc %}

# Introduction

In [part one](#) of this tutorial, you learned how to build a custom source connector for Flink. In part two, you will learn how to integrate the connector with a test email inbox through the IMAP protocol, filter out emails, and execute [Flink SQL on the Ververica Platform](https://www.ververica.com/apache-flink-sql-on-ververica-platform). 

# Goals

Part two of the tutorial will teach you how to: 

- integrate a source connector which connects to a mailbox using the IMAP protocol
- use [Jakarta Mail](https://eclipse-ee4j.github.io/mail/), a Java library that can send and receive email via the IMAP protocol  
- write [Flink SQL](https://ci.apache.org/projects/flink/flink-docs-release-1.13/docs/dev/table/sql/overview/) and execute the queries in the Ververica Platform

You are encouraged to follow along with the code in this [repository](github.com/Airblader/blog-imap). It provides a boilerplate project that also comes with a bundled [docker-compose](https://docs.docker.com/compose/) setup that lets you easily run the connector. You can then try it out with Flink’s SQL client.


# Prerequisites

This tutorial assumes that you have:

- followed the steps outlined in [part one](#) of this tutorial
- some familiarity with Java and objected-oriented programming


# Understand how to fetch emails via the IMAP protocol

Now that you have a working source connector that can run on Flink, it is time to connect to an email server via IMAP (an Internet protocol that allows email clients to retrieve messages from a mail server) so that Flink can process emails instead of test static data.  

You will use [Jakarta Mail](https://eclipse-ee4j.github.io/mail/), a Java library that can be used to send and receive email via IMAP. For simplicity, authentication will use a plain username and password.

This tutorial will focus more on how to implement a connector for Flink. If you want to learn more about the details of how IMAP or Jakarta Mail work, you are encouraged to explore a more extensive implementation at this [repository](github.com/Airblader/flink-connector-email). 

In order to fetch emails, you will need to connect to the email server, register a listener for new emails and collect them whenever they arrive, and enter a loop to keep the connector running. 


# Add configuration options - server information and credentials

In order to connect to your IMAP server, you will need at least the following:

- hostname (of the mail server)
- port number
- username
- password

You will start by creating a class to encapsulate the configuration options. You will make use of [Lombok](https://projectlombok.org/setup/maven) to help with some boilerplate code. By adding the `@Data` and `@Builder` annotations, Lombok will generate these for all the fields of the immutable class. 

```java
@Data
@Builder
public class ImapSourceOptions implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String host;
    private final Integer port;
    private final String user;
    private final String password;
}
```

Now you can add an instance of this class to the `ImapSourceFunction` and `ImapTableSource` classes so it can be used there. Take note of the column names with which the table has been created. This will help later.

// QUESTION: what would the column names be here??

```java
public class ImapSourceFunction extends RichSourceFunction<RowData> {
    private final ImapSourceOptions options;
    private final List<String> columnNames;

    public ImapSourceFunction(
        ImapSourceOptions options, 
        List<String> columnNames
    ) {
        this.options = options;
        this.columnNames = columnNames.stream()
            .map(String::toUpperCase)
            .collect(Collectors.toList());
    }

    // ...
}
```

```java
public class ImapTableSource implements ScanTableSource {

    private final ImapSourceOptions options;
    private final List<String> columnNames;

    public ImapTableSource(
        ImapSourceOptions options,
        List<String> columnNames
    ) {
        this.options = options;
        this.columnNames = columnNames;
    }

    // …

    @Override
    public ScanRuntimeProvider getScanRuntimeProvider(ScanContext ctx) {
        final ImapSourceFunction sourceFunction = new ImapSourceFunction(options, columnNames);
        return SourceFunctionProvider.of(sourceFunction, true);
    }

    @Override
    public DynamicTableSource copy() {
        return new ImapTableSource(options, columnNames);
    }

    // …
}
```

Finally, in the `ImapTableSourceFactory` class, you need to create a `ConfigOption<Type>Name` for the hostname, port number, username, and password.  Then you need to report them to Flink. Since all of the current options are mandatory, you can add them to the `requiredOptions()` method in order to do this. 

```java
public class ImapTableSourceFactory implements DynamicTableSourceFactory {

    public static final ConfigOption<String> HOST = ConfigOptions.key("host").stringType().noDefaultValue();
    public static final ConfigOption<Integer> PORT = ConfigOptions.key("port").intType().noDefaultValue();
    public static final ConfigOption<String> USER = ConfigOptions.key("user").stringType().noDefaultValue();
    public static final ConfigOption<String> PASSWORD = ConfigOptions.key("password").stringType().noDefaultValue();

    // …

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(HOST);
        options.add(PORT);
        options.add(USER);
        options.add(PASSWORD);
        return options;
    }

    // …
}
```

Now take a look at the `createDynamicTableSource()` function in the `ImapTableSouceFactory` class.  Recall that previously (in part one) you had created a small helper utility [TableFactoryHelper](https://ci.apache.org/projects/flink/flink-docs-master/api/java/org/apache/flink/table/factories/FactoryUtil.TableFactoryHelper.html), that Flink offers which ensures that required options are set and that no unknown options are provided. You can now use it to automatically make sure that the required options of hostname, port number, username, and password are all provided when creating a table using this connector. The helper function will throw an error message if one required option is missing. You can also use it to access the provided options (`getOptions()`), convert them into an instance of the `ImapTableSource` class created earlier, and provide the instance to the table source:

// why would you want to do the latter??

```java
public class ImapTableSourceFactory implements DynamicTableSourceFactory {

    // ...

    @Override
    public DynamicTableSource createDynamicTableSource(Context ctx) {
        final FactoryUtil.TableFactoryHelper factoryHelper = FactoryUtil.createTableFactoryHelper(this, ctx);
        factoryHelper.validate();

        final ImapSourceOptions options = ImapSourceOptions.builder()
            .host(factoryHelper.getOptions().get(HOST))
            .port(factoryHelper.getOptions().get(PORT))
            .user(factoryHelper.getOptions().get(USER))
            .password(factoryHelper.getOptions().get(PASSWORD))
            .build();
        final List<String> columnNames = ctx.getCatalogTable().getResolvedSchema().getColumnNames();
        return new ImapTableSource(options, columnNames);
    }
}
```

To test these new configuration options, run:

```sh
$ cd testing/
$ ./build_and_run.sh
```

Once you see the Flink SQL client start up, execute the following statements to create a table with your connector:

```sql
CREATE TABLE T (subject STRING, content STRING) WITH ('connector' = 'imap');

SELECT * FROM T;
```

This time it will fail because the required options are not provided.  

```
[ERROR] Could not execute SQL statement. Reason:
org.apache.flink.table.api.ValidationException: One or more required options are missing.

Missing required options are:

host
password
user
``` 


#  Connect to the source email server

Now that you have configured the required options to connect to the email server, it is time to actually connect to the server. 

Going back to the `ImapSourceFunction` class, you first need to convert the options given to the table source into a `Properties` object, which is what you can pass to the Jakarta library. You can also set various other properties here as well (i.e. enabling SSL).

// is there more information on this properties object??

```java
public class ImapSourceFunction extends RichSourceFunction<RowData> {
   // …

   private Properties getSessionProperties() {
        Properties props = new Properties();
        props.put("mail.store.protocol", "imap");
        props.put("mail.imap.auth", true);
        props.put("mail.imap.host", options.getHost());
        if (options.getPort() != null) {
            props.put("mail.imap.port", options.getPort());
        }

        return props;
    }
}
```

Now create a method (`connect()`) which sets up the connection:

```java 
public class ImapSourceFunction extends RichSourceFunction<RowData> {
    // …

    private transient Store store;
    private transient IMAPFolder folder;

    private void connect() throws Exception {
        var session = Session.getInstance(getSessionProperties(), null);
        store = session.getStore();
        store.connect(options.getUser(), options.getPassword());

        var genericFolder = store.getFolder("INBOX");
        folder = (IMAPFolder) genericFolder;

        if (!folder.isOpen()) {
            folder.open(Folder.READ_ONLY);
        }
    }
}
```

You can now use this method to connect to the mail server when the source is created. Create a loop to keep the source running while collecting email counts. Lastly, implement methods to cancel and close the connection:

```java
public class ImapSourceFunction extends RichSourceFunction<RowData> {
    private transient volatile boolean running = false;

    // …

    @Override
    public void run(SourceFunction.SourceContext<RowData> ctx) throws Exception {
        connect();
        running = true;

        // TODO: Listen for new messages

        while (running) {
            // Trigger some IMAP request to force the server to send a notification
            folder.getMessageCount();
            Thread.sleep(250);
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void close() throws Exception {
        if (folder != null) {
            folder.close();
        }

        if (store != null) {
            store.close();
        }
    }
}
```

There is a request trigger to the server in every loop iteration. This is crucial as it ensures that the server will keep sending notifications. A more sophisticated approach would be to make use of the IDLE protocol. 


## Collect incoming emails

Now you need to listen for new emails arriving in the inbox folder and collect them. To begin, hardcode the schema and only return the email’s subject. Fortunately, Jakarta provides a simple hook to get notified when new messages arrive on the server. You can use this in place of the “TODO” comment above:

```java
public class ImapSourceFunction extends RichSourceFunction<RowData> {
    @Override
    public void run(SourceFunction.SourceContext<RowData> ctx) throws Exception {
        // …

        folder.addMessageCountListener(new MessageCountAdapter() {
            @Override
            public void messagesAdded(MessageCountEvent e) {
                collectMessages(ctx, e.getMessages());
            }
        });

        // …
    }

    private void collectMessages(SourceFunction.SourceContext<RowData> ctx, Message[] messages) {
        for (Message message : messages) {
            try {
                ctx.collect(GenericRowData.of(StringData.fromString(message.getSubject())));
            } catch (MessagingException ignored) {}
        }
    }
}
```

We can now once again run build_and_run.sh to build the project and drop into the SQL client. This time, we’ll be connecting to a Greenmail server which is started as part of the setup:

```sql
CREATE TABLE T (
    subject STRING
) WITH (
    'connector' = 'imap', 
    'host' = 'greenmail',
    'port' = '3143', 
    'user' = 'alice', 
    'password' = 'alice'
);

SELECT * FROM T;
```

The query should now run continuously, but of course no rows will be produced. For that, we need to actually send an email to the server. If you have mailutils’ mailx installed, you can do so using

```java
$ echo "This is the email body" | mailx -Sv15-compat \
        -s"Test Subject" \
        -Smta=smtp://bob:bob@localhost:3025 \
        alice@acme.org

```

The rows “Test Subject” should now have appeared as a row in your output. Our source is working!

However, we’re still hard-coding the schema produced by the source, and e.g. defining the table with a different schema will produce errors. We want to be able to define which fields of an email interest us, however, and then produce the data accordingly. For this, we’ll use the list of column names we held onto earlier, and then simply look at it when we collect the emails. For brevity, we’ll only include a few of the possible fields here:

```java
private void collectMessages(SourceFunction.SourceContext<RowData> ctx, Message[] messages) {
        for (Message message : messages) {
            try {
                collectMessage(ctx, message);
            } catch (MessagingException ignored) {}
        }
    }

    private void collectMessage(SourceFunction.SourceContext<RowData> ctx, Message message)
        throws MessagingException {
        var row = new GenericRowData(columnNames.size());

        for (int i = 0; i < columnNames.size(); i++) {
            switch (columnNames.get(i)) {
                case "SUBJECT":
                    row.setField(i, StringData.fromString(message.getSubject()));
                    break;
                case "SENT":
                    row.setField(i, TimestampData.fromInstant(message.getSentDate().toInstant()));
                    break;
                case "RECEIVED":
                    row.setField(i, TimestampData.fromInstant(message.getReceivedDate().toInstant()));
                    break;
                // ...
            }
        }

        ctx.collect(row);
    }
```

You should now have a working source that we can select any of the columns from which we support. We can try it out once again, but this time specifying all the columns we support above:

```sql
CREATE TABLE T (
    subject STRING,
    sent TIMESTAMP(3),
    received TIMESTAMP(3)
) WITH (
    'connector' = 'imap', 
    'host' = 'greenmail',
    'port' = '3143', 
    'user' = 'alice', 
    'password' = 'alice'
);

SELECT * FROM T;
```

Use the command from earlier to send emails to the greenmail server and you should see them appear. You can also try selecting only some of the columns, or write more complex queries. Note, however, that there are quite a few more things we haven’t covered here, such as advancing watermarks.


# Test the connector with a real mail server on the Ververica Platform 

If you want to test the connector with a real mail server, you can import it into [Ververica Platform Community Edition](https://www.ververica.com/getting-started). 

Since our example connector in this blog post is still a bit limited, we’ll actually use github.com/Airblader/flink-connector-imap instead this time. We’ll also assume you already have Ververica Platform up and running (see the link above).

In this case, I will be connecting to a GMail account. This requires SSL, and comes with an additional caveat that you either need to enable unsafe apps (which tends to deactivate itself again frequently), or enable two-factor authentication and create an app password (this is more stable, and more safe). 

First, we head to SQL → Connectors. There we can create a new connector by uploading our JAR file. It’ll also detect the connector options automatically. Afterwards, we go back to the SQL Editor and should now be able to use the connector:

<div class="row front-graphic">
  <img src="{{ site.baseurl }}/img/blog/2021-07-07-backpressure/animated.png" alt="Backpressure monitoring in the web UI"/>
	<p class="align-center">image caption</p>
</div>


# Summary

Apache Flink allows users to access many different systems as data sources or sinks. The system is designed for very easy extensibility.

Apache Flink has a versatile set of connectors for externals data sources. It can read and write data from databases, local and distributed file systems. 

But Flink also exposes APIs on top of which custom connectors can be built.