package org.renci.irods_nsf;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributeView;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFileAttributeView;
import java.util.HashMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import javax.security.auth.Subject;

import org.dcache.nfs.ExportFile;
import org.dcache.nfs.status.NoEntException;
import org.dcache.nfs.v3.MountServer;
import org.dcache.nfs.v3.NfsServerV3;
import org.dcache.nfs.v4.DeviceManager;
import org.dcache.nfs.v4.MDSOperationFactory;
import org.dcache.nfs.v4.NFSServerV41;
import org.dcache.nfs.v4.NfsIdMapping;
import org.dcache.nfs.v4.SimpleIdMap;
import org.dcache.nfs.v4.xdr.nfsace4;
import org.dcache.nfs.vfs.AclCheckable;
import org.dcache.nfs.vfs.DirectoryEntry;
import org.dcache.nfs.vfs.FsStat;
import org.dcache.nfs.vfs.Inode;
import org.dcache.nfs.vfs.Stat;
import org.dcache.nfs.vfs.Stat.Type;
import org.dcache.nfs.vfs.VirtualFileSystem;
import org.dcache.xdr.OncRpcProgram;
import org.dcache.xdr.OncRpcSvc;
import org.dcache.xdr.OncRpcSvcBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Longs;

public class NSFExperiments {

    public static void main(String[] args) throws IOException
    {
        OncRpcSvc nfsSvc = new OncRpcSvcBuilder()
            .withPort(2049)
            .withTCP()
            .withAutoPublish()
            .withWorkerThreadIoStrategy()
            .build();
        
        ExportFile exportFile = new ExportFile(new File("config/exports"));
        VirtualFileSystem vfs = new MyVFS();
                
        NFSServerV41 nfs4 = new NFSServerV41(new MDSOperationFactory(),
                                             new DeviceManager(),
                                             vfs,
                                             exportFile);
        
        NfsServerV3 nfs3 = new NfsServerV3(exportFile, vfs);
        MountServer mountd = new MountServer(exportFile, vfs);
        
        nfsSvc.register(new OncRpcProgram(100003, 4), nfs4);
        nfsSvc.register(new OncRpcProgram(100003, 3), nfs3);
        nfsSvc.register(new OncRpcProgram(100003, 3), mountd);
        
        nfsSvc.start();
        
        System.in.read();
        
        nfsSvc.stop();
    }
	
    private static final class MyVFS implements VirtualFileSystem
    {
        private static final Logger LOG = LoggerFactory.getLogger(MyVFS.class);

        private final Map<Long, Path> inodeToPath = new HashMap<>();
        private final Map<Path, Long> pathToInode = new HashMap<>();
        private final AtomicLong fileId = new AtomicLong(1); //numbering starts at 1
        private final NfsIdMapping idMapper = new SimpleIdMap();
        
        public MyVFS() {
            map(fileId.getAndIncrement(), Paths.get("/home/kory"));
        }

        private Inode toFh(long inodeNumber) {
            return Inode.forFile(Longs.toByteArray(inodeNumber));
        }

        private long getInodeNumber(Inode inode) {
            return Longs.fromByteArray(inode.getFileId());
        }

        private Path resolveInode(long inodeNumber) throws NoEntException {
            Path path = inodeToPath.get(inodeNumber);
            if (path == null) {
                throw new NoEntException("inode #" + inodeNumber);
            }
            return path;
        }

        private long resolvePath(Path path) throws NoEntException {
            Long inodeNumber = pathToInode.get(path);
            if (inodeNumber == null) {
                throw new NoEntException("path " + path);
            }
            return inodeNumber;
        }

        private void map(long inodeNumber, Path path) {
            if (inodeToPath.putIfAbsent(inodeNumber, path) != null) {
                throw new IllegalStateException();
            }
            Long otherInodeNumber = pathToInode.putIfAbsent(path, inodeNumber);
            if (otherInodeNumber != null) {
                //try rollback
                if (inodeToPath.remove(inodeNumber) != path) {
                    throw new IllegalStateException("cant map, rollback failed");
                }
                throw new IllegalStateException("path ");
            }
        }

        private void unmap(long inodeNumber, Path path) {
            Path removedPath = inodeToPath.remove(inodeNumber);
            if (!path.equals(removedPath)) {
                throw new IllegalStateException();
            }
            if (pathToInode.remove(path) != inodeNumber) {
                throw new IllegalStateException();
            }
        }

        private void remap(long inodeNumber, Path oldPath, Path newPath) {
            //TODO - attempt rollback?
            unmap(inodeNumber, oldPath);
            map(inodeNumber, newPath);
        }

        public int access(Inode inode, int mode) throws IOException {
            System.out.println("access");
            System.out.println("\tinode = " + getInodeNumber(inode));
            System.out.println("\tmode  = " + mode);
            return mode;
        }

        public Inode create(Inode parent, Type type, String path, Subject subject, int mode) throws IOException {
            System.out.println("create");
            return null;
        }

        public FsStat getFsStat() throws IOException {
            System.out.println("getFsStat");
            return null;
        }

        public Inode getRootInode() throws IOException {
            System.out.println("getRootInode");
            return toFh(1); //always #1 (see constructor)
        }

        public Inode lookup(Inode parent, String path) throws IOException {
            System.out.println("lookup");
            System.out.println("parent inode = " + getInodeNumber(parent));
            System.out.println("path         = " + path);

            /*
            long newInodeNumber = fileId.getAndIncrement();
            Path parentPath = resolveInode(getInodeNumber(parent));

            map(newInodeNumber, Paths.get(parentPath.toString(), path));

            return toFh(newInodeNumber);
            */

            /*
            long parentInodeNumber = getInodeNumber(parent);
            Path parentPath = resolveInode(parentInodeNumber);
            Path child = parentPath.resolve(path);
            long childInodeNumber = resolvePath(child);
            return toFh(childInodeNumber);
            */
        }

        public Inode link(Inode parent, Inode link, String path, Subject subject) throws IOException {
            System.out.println("link");
            return null;
        }

        public List<DirectoryEntry> list(Inode inode) throws IOException {
            System.out.println("list");

            long inodeNumber = getInodeNumber(inode);
            Path path = resolveInode(inodeNumber);
            final List<DirectoryEntry> list = new ArrayList<>();
            
            Files.newDirectoryStream(path).forEach(p -> {
                try
                {
                    long cookie = resolvePath(p);
                    list.add(new DirectoryEntry(p.getFileName().toString(), toFh(cookie), statPath(p, cookie)));
                }
                catch (Exception e)
                {
                    throw new IllegalStateException(e);
                }
            });

            return list;
        }

        public Inode mkdir(Inode parent, String path, Subject subject, int mode) throws IOException {
            System.out.println("mkdir");
            return null;
        }

        public boolean move(Inode src, String oldName, Inode dest, String newName) throws IOException {
            System.out.println("move");
            return false;
        }

        public Inode parentOf(Inode inode) throws IOException {
            System.out.println("parentOf");
            return null;
        }

        public int read(Inode inode, byte[] data, long offset, int count) throws IOException {
            System.out.println("read");
            return 0;
        }

        public String readlink(Inode inode) throws IOException {
            System.out.println("readlink");
            return null;
        }

        public void remove(Inode parent, String path) throws IOException {
            System.out.println("remove");
            
        }

        public Inode symlink(Inode parent, String path, String link, Subject subject, int mode) throws IOException {
            System.out.println("symlink");
            return null;
        }

        public WriteResult write(Inode inode, byte[] data, long offset, int count, StabilityLevel stabilityLevel) throws IOException {
            System.out.println("write");
            return null;
        }

        public void commit(Inode inode, long offset, int count) throws IOException {
            System.out.println("commit");
            
        }

        public Stat getattr(Inode inode) throws IOException {
            System.out.println("getattr");
            System.out.println("inode = " + getInodeNumber(inode));
            long inodeNumber = getInodeNumber(inode);
            Path path = resolveInode(inodeNumber);
            return statPath(path, inodeNumber);
        }

        public void setattr(Inode inode, Stat stat) throws IOException {
            System.out.println("setattr");
            
        }

        public nfsace4[] getAcl(Inode inode) throws IOException {
            System.out.println("getAcl");
            return null;
        }

        public void setAcl(Inode inode, nfsace4[] acl) throws IOException {
            System.out.println("setAcl");
            
        }

        public boolean hasIOLayout(Inode inode) throws IOException {
            System.out.println("hasIOLayout");
            return false;
        }

        public AclCheckable getAclCheckable() {
            System.out.println("getAclCheckable");
            return null;
        }

        public NfsIdMapping getIdMapper() {
            System.out.println("getIdMapper");
            return idMapper;
        }

        private Stat statPath(Path p, long inodeNumber) throws IOException {
            Class<? extends  BasicFileAttributeView> attributeClass = PosixFileAttributeView.class;

            BasicFileAttributes attrs = Files.getFileAttributeView(p, attributeClass, LinkOption.NOFOLLOW_LINKS).readAttributes();

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
