package org.renci.irods_nsf;

import java.io.File;
import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.auth.Subject;

import org.dcache.nfs.ExportFile;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.v3.MountServer;
import org.dcache.nfs.v3.NfsServerV3;
import org.dcache.nfs.v4.MDSOperationFactory;
import org.dcache.nfs.v4.NFSServerV41;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.SimpleIdMap;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.AclCheckable;
import org.dcache.nfs.vfs.DirectoryEntry;
import org.dcache.nfs.vfs.DirectoryStream;
import org.dcache.nfs.vfs.FsStat;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.Stat.Type;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.oncrpc4j.rpc.OncRpcProgram;
import org.dcache.oncrpc4j.rpc.OncRpcSvc;
import org.dcache.oncrpc4j.rpc.OncRpcSvcBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.google.common.primitives.Longs;

public class NSFExperiments
{
    public static void main(String[] args) throws IOException
    {
        if (args.length != 1)
        {
            System.out.println("usage: app <root_directory>");
            return;
        }
        
        OncRpcSvc nfsSvc = new OncRpcSvcBuilder()
            .withPort(2049)
            .withTCP()
            .withAutoPublish()
            .withWorkerThreadIoStrategy().build();

        ExportFile exportFile = new ExportFile(new File("config/exports"));
        VirtualFileSystem vfs = new MyVFS(args[0]);

        NFSServerV41 nfs4 = new NFSServerV41.Builder()
            .withExportFile(exportFile)
            .withVfs(vfs)
            .withOperationFactory(new MDSOperationFactory())
            .build();

        NfsServerV3 nfs3 = new NfsServerV3(exportFile, vfs);
        MountServer mountd = new MountServer(exportFile, vfs);

        nfsSvc.register(new OncRpcProgram(100003, 4), nfs4);
        nfsSvc.register(new OncRpcProgram(100003, 3), nfs3);
        nfsSvc.register(new OncRpcProgram(100003, 3), mountd);

        nfsSvc.start();

        System.out.println("press [enter] to stop the server.");
        System.in.read();

        System.out.println("shutting down ...");
        nfsSvc.stop();
        
        System.out.println("shutdown complete.");
    }

    private static final class MyVFS implements VirtualFileSystem
    {
        private static final Logger log_ = LoggerFactory.getLogger(MyVFS.class);

        private final Path root_;
        private final Map<Long, Path> inodeToPath_ = new ConcurrentHashMap<>();
        private final Map<Path, Long> pathToInode_ = new ConcurrentHashMap<>();
        private final AtomicLong fileId_ = new AtomicLong(1); // numbering starts at 1
        private final NfsIdMapping idMapper_ = new SimpleIdMap();

        public MyVFS(String _root) throws IOException
        {
            root_ = Paths.get(_root);
            assert(Files.exists(root_));
            map(fileId_.getAndIncrement(), root_);
            
            // Walk only the root directory.
            // Afterwards, any new directories and files that are
            // encountered will be mapped into the VFS system.
            
            Files.walkFileTree(root_, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult preVisitDirectory(Path _dir, BasicFileAttributes _attrs) throws IOException
                {
                    if (root_.equals(_dir))
                        return FileVisitResult.CONTINUE;

                    if (!inodeToPath_.containsValue(_dir))
                        map(fileId_.incrementAndGet(), _dir);

                    return FileVisitResult.SKIP_SUBTREE;
                }
                
                @Override
                public FileVisitResult visitFile(Path _file, BasicFileAttributes _attrs) throws IOException
                {
                    if (!inodeToPath_.containsValue(_file))
                        map(fileId_.incrementAndGet(), _file);

                    return FileVisitResult.CONTINUE;
                }
            });
            
            ObjectMapper mapper = new ObjectMapper();
            mapper.enable(SerializationFeature.INDENT_OUTPUT);

            System.out.println("inode-path map ==> " + mapper.writeValueAsString(inodeToPath_));
        }

        @Override
        public int access(Inode inode, int mode) throws IOException
        {
            System.out.println("vfs::access");
            return mode;
        }

        @Override
        public void commit(Inode inode, long offset, int count) throws IOException
        {
            System.out.println("vfs::commit");
        }

        @Override
        public Inode create(Inode parent, Type type, String name, Subject subject, int mode) throws IOException
        {
            System.out.println("vfs::create");
            return null;
        }

        @Override
        public byte[] directoryVerifier(Inode inode) throws IOException
        {
            System.out.println("vfs::directoryVerifier");
            return null;
        }

        @Override
        public nfsace4[] getAcl(Inode inode) throws IOException
        {
            System.out.println("vfs::getAcl");
            return new nfsace4[0];
        }

        @Override
        public AclCheckable getAclCheckable()
        {
            System.out.println("vfs::getAclCheckable");
            return null;
        }

        @Override
        public FsStat getFsStat() throws IOException
        {
            System.out.println("vfs::getFsStat");
            return null;
        }

        @Override
        public NfsIdMapping getIdMapper()
        {
            System.out.println("vfs::getIdMapper");
            return idMapper_;
        }

        @Override
        public Inode getRootInode() throws IOException
        {
            System.out.println("vfs::getRootInode");
            return toFh(1);
        }

        @Override
        public Stat getattr(Inode inode) throws IOException
        {
            System.out.println("vfs::getattr");
            long inodeNumber = getInodeNumber(inode);
            Path path = resolveInode(inodeNumber);
            System.out.println("\tinode-path = " + inodeNumber + "-" + path);
            return statPath(path, inodeNumber);
        }

        @Override
        public boolean hasIOLayout(Inode inode) throws IOException
        {
            System.out.println("vfs::hasIOLayout");
            return false;
        }

        @Override
        public Inode link(Inode parent, Inode link, String name, Subject subject) throws IOException
        {
            System.out.println("vfs::link");
            return null;
        }

        @Override
        public DirectoryStream list(Inode inode, byte[] verifier, long cookie) throws IOException
        {
            System.out.println("vfs::list");
            System.out.println("inode = " + getInodeNumber(inode));
            
            Path path = resolveInode(getInodeNumber(inode));
            System.out.println("path  = " + path);
            
            final List<DirectoryEntry> list = new ArrayList<>();
            
            // Walk the contents of the inode.
            // Map all new files and directories into the tables.
            Files.newDirectoryStream(path).forEach(p -> {
                try
                {
                    long inodeNumber = -1;
                    String filename = p.getFileName().toString();
                    
                    if (!inodeToPath_.containsValue(p))
                    {
                        inodeNumber = fileId_.incrementAndGet();
                        map(inodeNumber, p);
                    }
                    else
                    {
                        inodeNumber = resolvePath(p);
                    }

                    list.add(new DirectoryEntry(filename, toFh(inodeNumber), statPath(p, inodeNumber), inodeNumber));
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                    list.clear();
                }
            });
            
            return new DirectoryStream(list);
        }

        @Override
        public Inode lookup(Inode parent, String name) throws IOException
        {
            System.out.println("vfs::lookup");
            
            Path parentPath = resolveInode(getInodeNumber(parent));
            Path filePath = parentPath.resolve(name);
            
            return toFh(resolvePath(filePath));
        }

        @Override
        public Inode mkdir(Inode parent, String name, Subject subject, int mode) throws IOException
        {
            System.out.println("vfs::mkdir");
            return null;
        }

        @Override
        public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException
        {
            System.out.println("vfs::move");
            return false;
        }

        @Override
        public Inode parentOf(Inode inode) throws IOException
        {
            System.out.println("vfs::parentOf");
            return null;
        }

        @Override
        public int read(Inode inode, byte[] data, long offset, int count) throws IOException
        {
            System.out.println("vfs::read");
            return 0;
        }

        @Override
        public String readlink(Inode inode) throws IOException
        {
            System.out.println("vfs::readlink");
            return null;
        }

        @Override
        public void remove(Inode parent, String name) throws IOException
        {
            System.out.println("vfs::remove");
        }

        @Override
        public void setAcl(Inode inode, nfsace4[] acl) throws IOException
        {
            System.out.println("vfs::setAcl");
        }

        @Override
        public void setattr(Inode inode, Stat stat) throws IOException
        {
            System.out.println("vfs::setattr");
        }

        @Override
        public Inode symlink(Inode parent, String name, String link, Subject subject, int mode) throws IOException
        {
            System.out.println("vfs::symlink");
            return null;
        }

        @Override
        public WriteResult write(Inode inode, byte[] data, long offset, int count, StabilityLevel stabilityLevel)
            throws IOException
        {
            System.out.println("vfs::write");
            return null;
        }

        private Inode toFh(long inodeNumber)
        {
            return Inode.forFile(Longs.toByteArray(inodeNumber));
        }

        private long getInodeNumber(Inode inode)
        {
            return Longs.fromByteArray(inode.getFileId());
        }

        private Path resolveInode(long inodeNumber) throws NoEntException
        {
            Path path = inodeToPath_.get(inodeNumber);

            if (path == null)
                throw new NoEntException("inode #" + inodeNumber);

            return path;
        }

        private long resolvePath(Path path) throws NoEntException
        {
            Long inodeNumber = pathToInode_.get(path);

            if (inodeNumber == null)
                throw new NoEntException("path " + path);

            return inodeNumber;
        }

        private void map(long inodeNumber, Path path)
        {
            if (inodeToPath_.putIfAbsent(inodeNumber, path) != null)
                throw new IllegalStateException();

            Long otherInodeNumber = pathToInode_.putIfAbsent(path, inodeNumber);

            if (otherInodeNumber != null)
            {
                // try rollback
                if (inodeToPath_.remove(inodeNumber) != path)
                    throw new IllegalStateException("cant map, rollback failed");

                throw new IllegalStateException("path ");
            }
        }

        private void unmap(long inodeNumber, Path path)
        {
            Path removedPath = inodeToPath_.remove(inodeNumber);

            if (!path.equals(removedPath))
                throw new IllegalStateException();

            if (pathToInode_.remove(path) != inodeNumber)
                throw new IllegalStateException();
        }

        private void remap(long inodeNumber, Path oldPath, Path newPath)
        {
            unmap(inodeNumber, oldPath);
            map(inodeNumber, newPath);
        }

        private Stat statPath(Path p, long inodeNumber) throws IOException
        {
            Class<? extends BasicFileAttributeView> attributeClass = PosixFileAttributeView.class;

            BasicFileAttributes attrs = Files.getFileAttributeView(p, attributeClass, LinkOption.NOFOLLOW_LINKS)
                .readAttributes();

            Stat stat = new Stat();

            stat.setATime(attrs.lastAccessTime().toMillis());
            stat.setCTime(attrs.creationTime().toMillis());
            stat.setMTime(attrs.lastModifiedTime().toMillis());

            stat.setGid((Integer) Files.getAttribute(p, "unix:gid", LinkOption.NOFOLLOW_LINKS));
            stat.setUid((Integer) Files.getAttribute(p, "unix:uid", LinkOption.NOFOLLOW_LINKS));
            stat.setMode((Integer) Files.getAttribute(p, "unix:mode", LinkOption.NOFOLLOW_LINKS));
            stat.setNlink((Integer) Files.getAttribute(p, "unix:nlink", LinkOption.NOFOLLOW_LINKS));

            stat.setDev(17);
            stat.setIno((int) inodeNumber);
            stat.setRdev(17);
            stat.setSize(attrs.size());
            stat.setFileid((int) inodeNumber);
            stat.setGeneration(attrs.lastModifiedTime().toMillis());

            return stat;
        }
    }
}
