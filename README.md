teapot
======
Hi there!
Thank you for using BackuPOT (BackuP On Teapot). I know that this readme is
ridiculously long. But it's a prototype and fragile. Please read it before
you try to use it.

Prerequisite:
1. Generate configuration.
You can run ./config generate <nodeId> <passphrase> <ip:port>
The ip:port is used to setup a local Teapot sever to server p2p log
exchange request.  Most of the time, this ip:port pair is useless. It only
matters when the system is in p2p mode.

This command will generate two files. One is called teapot.config, the
other is called teapot.pub.<nodeId>. The teapot.pub.<nodeId> file should be
exchanged (see next) with others so that different nodes can do log
exchange with each other.

2. Exchange configuration with others.
you can run ./config add teapot.config teapot.pub.<nodeId> to
incorporate node information for <nodeId> into your teapot.config.
Be sure to backup the old teapot.config manually if you find it necessary.

3. Start server by ./teapot-daemon.sh start
The script will automatically update your local Teapot binaries. Also, it
will download necessary binaries and prompt you to interactively generate
configuration for you if you failed to do the first step.

Usage:
1. ./backupot.sh backup <local_path> <remote_path>
Backup the file specified by <local_path> to <remote_path>.
You can read more on how remote_path is structured in Note:1.

2. ./backupot.sh snapshot
Take a snapshot of current system state. Need to take some time for other
clients to respond to the request. The version of the snapshot will be
displayed by the version command (3.) when it finishes. Currently you have
to check it manually to know if the snapshot finishes or not. If you are
interested, snapshot command is implemented with garbage collection of
Teapot.
If you don't take a snapshot, backup command will always overwrite the
current version. You can either restore the current version or any other
previous finished snapshots with restore (4.) command.

3. ./backupot.sh version
List the time stamp when a snapshot is taken and its index number which can
be used in restore command to specify a version to restore.
Note that -1 denotes the latest backup that is not snapshotted yet.

4. ./backupot.sh restore <remote_path> <local_path> <version>
Restore a file located by <remote_path> to the path specified by
<local_path>. <version> should be one of the order number listed by version
command.  But you don't need to call version every time you want to restore
a file as long as you can remember the version number you want to restore.
If you type -1 as version number it will restore the latest version.

5. ./backupot.sh chmod <directory> <passphrase> <nodeId>+(r|w) ...
"r" is for read access and "w" is for write access.
You can put multiple <nodeId> and access information in this command.
The granularity of access control is directory. By default, you can only
read/write remotePath starting with your own <nodeId> <directory> is part
of the remote_path, please see "Note 2" below for details.

Note:
1. In order to support directory, remote_path should be structured in the
following form:
<nodeId>/<directory>/<path>

<nodeId> should be a valid string consisting only of letters and digits. By
default, you can only update remote paths that starts with your own
<nodeId>.

<directory> should be a string containing only letters and digits. Note
that only this part should be put in chmod command.
<path> can be a string containing letters, digits, dots and slashes.

2. Again, the granularity of sharing is directory. If user A shares
directory foo with user B, then B can access every path inside directory
foo.

3. Remember to stop server by ./teapot-daemon.sh stop when you finished
testing it. Or the server will update the beacon object periodically and
Amazon is going to charge for that. :)

Use Case:
1. Single User
a) Make sure you follow every step in Prerequisite.

b) Start to use ./backupot command to backup and restore files.

2. Multiple Users
a) Generate configuration for on each user. Ask each user to execute the
   following command on his/her machine.
   ./config generate <nodeId> <passphrase> <ip:port>
   for example,
   Alice runs ./config generate <Alice's_nodeId> <Alice's_passphrase> <Alice's_ip:port>
   on her machine.
   Bob runs ./config generate <Bob's_nodeId> <Bob's_passphrase> <Bob's_ip:port> on his machine.

   At this point, two files will be generated. One is called teapot.config.
   The other is called teapot.pub.<nodeId>.

b) Everyone put teapot.pub.<nodeId> in a public place.

c) Get others' public configuration from the public repo.

d) Add others' public configuration to one's own configuration.
   e.g. for Alice:
   ./config add teapot.config teapot.pub.<Bob's_nodeId>

e) Choose one primary user. Say Alice in this example.

f) Execute the following command on Alice's machine to give Bob write
   access.
   ./backupot chmod <shared_dir> <passphrase> <Bob's_nodeId>+w

g) Now Bob can read from/write to the remote dir located at
   Alice's_node_id/shared_dir/
