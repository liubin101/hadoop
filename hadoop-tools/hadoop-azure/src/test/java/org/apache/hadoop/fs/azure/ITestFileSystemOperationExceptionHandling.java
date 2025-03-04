/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.fs.azure;

import java.io.FileNotFoundException;
import java.io.IOException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.contract.ContractTestUtils;
import org.apache.hadoop.fs.permission.FsAction;
import org.apache.hadoop.fs.permission.FsPermission;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.apache.hadoop.fs.FSExceptionMessages.STREAM_IS_CLOSED;
import static org.apache.hadoop.fs.azure.ExceptionHandlingTestHelper.*;
import static org.apache.hadoop.test.LambdaTestUtils.intercept;

/**
 * Single threaded exception handling.
 */
public class ITestFileSystemOperationExceptionHandling
    extends AbstractWasbTestBase {

  private FSDataInputStream inputStream = null;

  private Path testPath;
  private Path testFolderPath;

  @BeforeEach
  @Override
  public void setUp() throws Exception {
    super.setUp();
    testPath = path("testfile.dat");
    testFolderPath = path("testfolder");
  }

  /**
   * Helper method that creates a InputStream to validate exceptions
   * for various scenarios.
   */
  private void setupInputStreamToTest(AzureBlobStorageTestAccount testAccount)
      throws Exception {

    FileSystem fs = testAccount.getFileSystem();

    // Step 1: Create a file and write dummy data.
    Path base = methodPath();
    Path testFilePath1 = new Path(base, "test1.dat");
    Path testFilePath2 = new Path(base, "test2.dat");
    FSDataOutputStream outputStream = fs.create(testFilePath1);
    String testString = "This is a test string";
    outputStream.write(testString.getBytes());
    outputStream.close();

    // Step 2: Open a read stream on the file.
    inputStream = fs.open(testFilePath1);

    // Step 3: Rename the file
    fs.rename(testFilePath1, testFilePath2);
  }

  /**
   * Tests a basic single threaded read scenario for Page blobs.
   */
  @Test
  public void testSingleThreadedPageBlobReadScenario() throws Throwable {
    assertThrows(FileNotFoundException.class, () -> {
      AzureBlobStorageTestAccount testAccount = getPageBlobTestStorageAccount();
      setupInputStreamToTest(testAccount);
      byte[] readBuffer = new byte[512];
      inputStream.read(readBuffer);
    });
  }

  /**
   * Tests a basic single threaded seek scenario for Page blobs.
   */
  @Test
  public void testSingleThreadedPageBlobSeekScenario() throws Throwable {
    assertThrows(FileNotFoundException.class, () -> {
      AzureBlobStorageTestAccount testAccount = getPageBlobTestStorageAccount();
      setupInputStreamToTest(testAccount);
      inputStream.seek(5);
    });
  }

  /**
   * Test a basic single thread seek scenario for Block blobs.
   */
  @Test
  public void testSingleThreadBlockBlobSeekScenario() throws Throwable {
    assertThrows(FileNotFoundException.class, () -> {
      AzureBlobStorageTestAccount testAccount = createTestAccount();
      setupInputStreamToTest(testAccount);
      inputStream.seek(5);
      inputStream.read();
    });
  }

  /**
   * Tests a basic single threaded read scenario for Block blobs.
   */
  @Test
  public void testSingledThreadBlockBlobReadScenario() throws Throwable {
    assertThrows(FileNotFoundException.class, () -> {
      AzureBlobStorageTestAccount testAccount = createTestAccount();
      setupInputStreamToTest(testAccount);
      byte[] readBuffer = new byte[512];
      inputStream.read(readBuffer);
    });
  }

  /**
   * Tests basic single threaded setPermission scenario.
   */
  @Test
  public void testSingleThreadedBlockBlobSetPermissionScenario() throws Throwable {
    assertThrows(FileNotFoundException.class, () -> {
      createEmptyFile(createTestAccount(), testPath);
      fs.delete(testPath, true);
      fs.setPermission(testPath,
         new FsPermission(FsAction.EXECUTE, FsAction.READ, FsAction.READ));
    });
  }

  /**
   * Tests basic single threaded setPermission scenario.
   */
  @Test
  public void testSingleThreadedPageBlobSetPermissionScenario()
      throws Throwable {
    assertThrows(FileNotFoundException.class, () -> {
      createEmptyFile(getPageBlobTestStorageAccount(), testPath);
      fs.delete(testPath, true);
      fs.setOwner(testPath, "testowner", "testgroup");
    });
  }

  /**
   * Tests basic single threaded setPermission scenario.
   */
  @Test
  public void testSingleThreadedBlockBlobSetOwnerScenario() throws Throwable {
    assertThrows(FileNotFoundException.class, () -> {
      createEmptyFile(createTestAccount(), testPath);
      fs.delete(testPath, true);
      fs.setOwner(testPath, "testowner", "testgroup");
    });
  }

  /**
   * Tests basic single threaded setPermission scenario.
   */
  @Test
  public void testSingleThreadedPageBlobSetOwnerScenario() throws Throwable {
    assertThrows(FileNotFoundException.class, ()->{
      createEmptyFile(getPageBlobTestStorageAccount(), testPath);
      fs.delete(testPath, true);
      fs.setPermission(testPath,
          new FsPermission(FsAction.EXECUTE, FsAction.READ, FsAction.READ));
    });
  }

  /**
   * Test basic single threaded listStatus scenario.
   */
  @Test
  public void testSingleThreadedBlockBlobListStatusScenario() throws Throwable {
    assertThrows(FileNotFoundException.class, () -> {
      createTestFolder(createTestAccount(), testFolderPath);
      fs.delete(testFolderPath, true);
      fs.listStatus(testFolderPath);
    });
  }

  /**
   * Test basic single threaded listStatus scenario.
   */
  @Test
  public void testSingleThreadedPageBlobListStatusScenario() throws Throwable {
    assertThrows(FileNotFoundException.class, () -> {
      createTestFolder(getPageBlobTestStorageAccount(), testFolderPath);
      fs.delete(testFolderPath, true);
      fs.listStatus(testFolderPath);
    });
  }

  /**
   * Test basic single threaded listStatus scenario.
   */
  @Test
  public void testSingleThreadedBlockBlobRenameScenario() throws Throwable {

    createEmptyFile(createTestAccount(),
        testPath);
    Path dstPath = new Path("dstFile.dat");
    fs.delete(testPath, true);
    boolean renameResult = fs.rename(testPath, dstPath);
    assertFalse(renameResult);
  }

  /**
   * Test basic single threaded listStatus scenario.
   */
  @Test
  public void testSingleThreadedPageBlobRenameScenario() throws Throwable {

    createEmptyFile(getPageBlobTestStorageAccount(),
        testPath);
    Path dstPath = new Path("dstFile.dat");
    fs.delete(testPath, true);
    boolean renameResult = fs.rename(testPath, dstPath);
    assertFalse(renameResult);
  }

  /**
   * Test basic single threaded listStatus scenario.
   */
  @Test
  public void testSingleThreadedBlockBlobDeleteScenario() throws Throwable {

    createEmptyFile(createTestAccount(),
        testPath);
    fs.delete(testPath, true);
    boolean deleteResult = fs.delete(testPath, true);
    assertFalse(deleteResult);
  }

  /**
   * Test basic single threaded listStatus scenario.
   */
  @Test
  public void testSingleThreadedPageBlobDeleteScenario() throws Throwable {

    createEmptyFile(getPageBlobTestStorageAccount(),
        testPath);
    fs.delete(testPath, true);
    boolean deleteResult = fs.delete(testPath, true);
    assertFalse(deleteResult);
  }

  /**
   * Test basic single threaded listStatus scenario.
   */
  @Test
  public void testSingleThreadedBlockBlobOpenScenario() throws Throwable {
    assertThrows(FileNotFoundException.class, () -> {
      createEmptyFile(createTestAccount(), testPath);
      fs.delete(testPath, true);
      inputStream = fs.open(testPath);
    });
  }

  /**
   * Test delete then open a file.
   */
  @Test
  public void testSingleThreadedPageBlobOpenScenario() throws Throwable {
    assertThrows(FileNotFoundException.class, ()->{
      createEmptyFile(getPageBlobTestStorageAccount(), testPath);
      fs.delete(testPath, true);
      inputStream = fs.open(testPath);
    });
  }

  /**
   * Attempts to write to the azure stream after it is closed will raise
   * an IOException.
   */
  @Test
  public void testWriteAfterClose() throws Throwable {
    FSDataOutputStream out = fs.create(testPath);
    out.close();
    intercept(IOException.class, STREAM_IS_CLOSED,
        () -> out.write('a'));
    intercept(IOException.class, STREAM_IS_CLOSED,
        () -> out.write(new byte[]{'a'}));
    out.hsync();
    out.flush();
    out.close();
  }

  @AfterEach
  public void tearDown() throws Exception {
    if (inputStream != null) {
      inputStream.close();
    }

    ContractTestUtils.rm(fs, testPath, true, true);
    super.tearDown();
  }

  @Override
  protected AzureBlobStorageTestAccount createTestAccount()
      throws Exception {
    return AzureBlobStorageTestAccount.create();
  }
}
