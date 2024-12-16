/*! ******************************************************************************
 *
 * Pentaho
 *
 * Copyright (C) 2024 by Hitachi Vantara, LLC : http://www.pentaho.com
 *
 * Use of this software is governed by the Business Source License included
 * in the LICENSE.TXT file.
 *
 * Change Date: 2029-07-20
 ******************************************************************************/


package org.pentaho.platform.plugin.services.importer;

import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.logging.Log;
import org.pentaho.database.model.IDatabaseConnection;
import org.pentaho.metadata.repository.DomainAlreadyExistsException;
import org.pentaho.metadata.repository.DomainIdNullException;
import org.pentaho.metadata.repository.DomainStorageException;
import org.pentaho.platform.api.engine.security.userroledao.AlreadyExistsException;
import org.pentaho.platform.api.engine.security.userroledao.IPentahoRole;
import org.pentaho.platform.api.engine.security.userroledao.IUserRoleDao;
import org.pentaho.platform.api.mimetype.IMimeType;
import org.pentaho.platform.api.mt.ITenant;
import org.pentaho.platform.api.repository.datasource.IDatasourceMgmtService;
import org.pentaho.platform.api.repository2.unified.IPlatformImportBundle;
import org.pentaho.platform.api.repository2.unified.IUnifiedRepository;
import org.pentaho.platform.api.repository2.unified.RepositoryFile;
import org.pentaho.platform.api.scheduler2.IJob;
import org.pentaho.platform.api.scheduler2.IJobRequest;
import org.pentaho.platform.api.scheduler2.IJobScheduleParam;
import org.pentaho.platform.api.scheduler2.IJobScheduleRequest;
import org.pentaho.platform.api.scheduler2.IScheduler;
import org.pentaho.platform.api.scheduler2.ISchedulerResource;
import org.pentaho.platform.api.scheduler2.JobState;
import org.pentaho.platform.api.usersettings.IAnyUserSettingService;
import org.pentaho.platform.api.usersettings.IUserSettingService;
import org.pentaho.platform.api.usersettings.pojo.IUserSetting;
import org.pentaho.platform.core.mt.Tenant;
import org.pentaho.platform.engine.core.system.PentahoSystem;
import org.pentaho.platform.engine.core.system.TenantUtils;
import org.pentaho.platform.plugin.services.importexport.DatabaseConnectionConverter;
import org.pentaho.platform.plugin.services.importexport.ExportFileNameEncoder;
import org.pentaho.platform.plugin.services.importexport.ExportManifestUserSetting;
import org.pentaho.platform.plugin.services.importexport.ImportSession;
import org.pentaho.platform.plugin.services.importexport.ImportSession.ManifestFile;
import org.pentaho.platform.plugin.services.importexport.ImportSource.IRepositoryFileBundle;
import org.pentaho.platform.plugin.services.importexport.RepositoryFileBundle;
import org.pentaho.platform.plugin.services.importexport.RoleExport;
import org.pentaho.platform.plugin.services.importexport.UserExport;
import org.pentaho.platform.plugin.services.importexport.exportManifest.ExportManifest;
import org.pentaho.platform.plugin.services.importexport.exportManifest.Parameters;
import org.pentaho.platform.plugin.services.importexport.exportManifest.bindings.ExportManifestMetaStore;
import org.pentaho.platform.plugin.services.importexport.exportManifest.bindings.ExportManifestMetadata;
import org.pentaho.platform.plugin.services.importexport.exportManifest.bindings.ExportManifestMondrian;
import org.pentaho.platform.plugin.services.importexport.legacy.MondrianCatalogRepositoryHelper;
import org.pentaho.platform.plugin.services.messages.Messages;
import org.pentaho.platform.repository.RepositoryFilenameUtils;
import org.pentaho.platform.security.policy.rolebased.IRoleAuthorizationPolicyRoleBindingDao;
import org.pentaho.platform.web.http.api.resources.services.FileService;

import javax.ws.rs.core.Response;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class SolutionImportHandler implements IPlatformImportHandler {

  private static final String RESERVEDMAPKEY_LINEAGE_ID = "lineage-id";

  private static final String XMI_EXTENSION = ".xmi";

  private static final String EXPORT_MANIFEST_XML_FILE = "exportManifest.xml";
  private static final String DOMAIN_ID = "domain-id";
  private static final String UTF_8 = StandardCharsets.UTF_8.name();

  private IUnifiedRepository repository; // TODO inject via Spring
  protected Map<String, RepositoryFileImportBundle.Builder> cachedImports;
  private SolutionFileImportHelper solutionHelper;
  private List<IMimeType> mimeTypes;
  private boolean overwriteFile;
  private List<IRepositoryFileBundle> files;

  public SolutionImportHandler( List<IMimeType> mimeTypes ) {
    this.mimeTypes = mimeTypes;
    this.solutionHelper = new SolutionFileImportHelper();
    repository = PentahoSystem.get( IUnifiedRepository.class );
  }

  public ImportSession getImportSession() {
    return ImportSession.getSession();
  }

  public Log getLogger() {
    return getImportSession().getLogger();
  }

  @Override
  public void importFile( IPlatformImportBundle bundle ) throws PlatformImportException, DomainIdNullException,
    DomainAlreadyExistsException, DomainStorageException, IOException {
    getLogger().info("Starting the import process");
    RepositoryFileImportBundle importBundle = (RepositoryFileImportBundle) bundle;
    // Processing file
    getLogger().debug(" Start  pre processing files and folder from import bundle");
    if ( !processZip( bundle.getInputStream() ) ) {
      // Something went wrong, do not proceed!
      return;
    }
    getLogger().debug(" End  pre processing files and folder from import bundle");

    setOverwriteFile( bundle.overwriteInRepository() );
    cachedImports = new HashMap<>();

    //Process Manifest Settings
    ExportManifest manifest = getImportSession().getManifest();
    // Process Metadata
    if ( manifest != null ) {
      // import the users
      Map<String, List<String>> roleToUserMap = importUsers( manifest.getUserExports() );

      // import the roles
      importRoles( manifest.getRoleExports(), roleToUserMap );

      // import the metadata
      importMetadata( manifest.getMetadataList(), bundle.isPreserveDsw() );

      // Process Mondrian
      importMondrian( manifest.getMondrianList() );

      // import the metastore
      importMetaStore( manifest.getMetaStore(), bundle.overwriteInRepository() );

      // import jdbc datasource
      importJDBCDataSource( manifest );
    }
    // import files and folders
    importRepositoryFilesAndFolders( manifest, bundle );

    // import schedules
    if ( manifest != null ) {
      importSchedules( manifest.getScheduleList() );
    }
  }

  protected void importRepositoryFilesAndFolders(ExportManifest manifest, IPlatformImportBundle bundle ) throws IOException {
    getLogger().info( "*********************** [ Start: Import File/Folder(s) ] **************************************" );
    getLogger().info( "Found  [ " +  files.size() + "] files to import" );
    int successfulFilesImportCount = 0;
    String manifestVersion = null;
    if ( manifest != null ) {
      manifestVersion = manifest.getManifestInformation().getManifestVersion();
    }
    RepositoryFileImportBundle importBundle = (RepositoryFileImportBundle) bundle;

    LocaleFilesProcessor localeFilesProcessor = new LocaleFilesProcessor();
    IPlatformImporter importer = PentahoSystem.get( IPlatformImporter.class );

    for ( IRepositoryFileBundle fileBundle : files ) {
      String fileName = fileBundle.getFile().getName();
      String actualFilePath = fileBundle.getPath();
      if ( manifestVersion != null ) {
        fileName = ExportFileNameEncoder.decodeZipFileName( fileName );
        actualFilePath = ExportFileNameEncoder.decodeZipFileName( actualFilePath );
      }
      String repositoryFilePath =
              RepositoryFilenameUtils.concat( PentahoPlatformImporter.computeBundlePath( actualFilePath ), fileName );

      if ( cachedImports.containsKey( repositoryFilePath ) ) {
        getLogger().debug("Repository object with path [ " + repositoryFilePath + " ] found in the cache");
        byte[] bytes = IOUtils.toByteArray( fileBundle.getInputStream() );
        RepositoryFileImportBundle.Builder builder = cachedImports.get( repositoryFilePath );
        builder.input( new ByteArrayInputStream( bytes ) );

        try {
          importer.importFile( build( builder ) );
          getLogger().debug("Successfully imported repository object with path [ " + repositoryFilePath + " ] from the cache");
          successfulFilesImportCount++;
          continue;
        } catch (PlatformImportException e) {
          getLogger().error("Error importing repository object with path [ " + repositoryFilePath + " ] from the cache. Cause [ " + e.getLocalizedMessage() + " ]");
        }
      }

      RepositoryFileImportBundle.Builder bundleBuilder = new RepositoryFileImportBundle.Builder();
      InputStream bundleInputStream = null;

      String decodedFilePath = fileBundle.getPath();
      RepositoryFile decodedFile = fileBundle.getFile();
      if ( manifestVersion != null ) {
        decodedFile = new RepositoryFile.Builder( decodedFile ).path( decodedFilePath ).name( fileName ).title( fileName ).build();
        decodedFilePath = ExportFileNameEncoder.decodeZipFileName( fileBundle.getPath() );
      }

      if ( fileBundle.getFile().isFolder() ) {
        bundleBuilder.mime( "text/directory" );
        bundleBuilder.file( decodedFile );
        fileName = repositoryFilePath;
        repositoryFilePath = importBundle.getPath();
      } else {
        byte[] bytes = IOUtils.toByteArray( fileBundle.getInputStream() );
        bundleInputStream = new ByteArrayInputStream( bytes );
        // If is locale file store it for later processing.
        if ( localeFilesProcessor.isLocaleFile( fileBundle, importBundle.getPath(), bytes ) ) {
          getLogger().trace( Messages.getInstance()
                  .getString( "SolutionImportHandler.SkipLocaleFile",  repositoryFilePath ) );
          continue;
        }
        bundleBuilder.input( bundleInputStream );
        bundleBuilder.mime( solutionHelper.getMime( fileName ) );

        String filePath =
                ( decodedFilePath.equals( "/" ) || decodedFilePath.equals( "\\" ) ) ? "" : decodedFilePath;
        repositoryFilePath = RepositoryFilenameUtils.concat( importBundle.getPath(), filePath );
      }

      bundleBuilder.name( fileName );
      bundleBuilder.path( repositoryFilePath );

      String sourcePath;
      if ( fileBundle.getFile().isFolder() ) {
        sourcePath = fileName;
      } else {
        sourcePath =
                RepositoryFilenameUtils.concat( PentahoPlatformImporter.computeBundlePath( actualFilePath ), fileName );
      }

      //This clause was added for processing ivb files so that it would not try process acls on folders that the user
      //may not have rights to such as /home or /public
      if ( manifest != null && manifest.getExportManifestEntity( sourcePath ) == null && fileBundle.getFile()
              .isFolder() ) {
        continue;
      }

      getImportSession().setCurrentManifestKey( sourcePath );

      bundleBuilder.charSet( bundle.getCharSet() );
      bundleBuilder.overwriteFile( bundle.overwriteInRepository() );
      bundleBuilder.applyAclSettings( bundle.isApplyAclSettings() );
      bundleBuilder.retainOwnership( bundle.isRetainOwnership() );
      bundleBuilder.overwriteAclSettings( bundle.isOverwriteAclSettings() );
      bundleBuilder.acl( getImportSession().processAclForFile( sourcePath ) );
      bundleBuilder.extraMetaData( getImportSession().processExtraMetaDataForFile( sourcePath ) );

      RepositoryFile file = getFile( importBundle, fileBundle );
      ManifestFile manifestFile = getImportSession().getManifestFile( sourcePath, file != null );

      bundleBuilder.hidden( isFileHidden( file, manifestFile, sourcePath ) );
      boolean isSchedulable = isSchedulable( file, manifestFile );

      if ( isSchedulable ) {
        bundleBuilder.schedulable( isSchedulable );
      } else {
        bundleBuilder.schedulable( fileIsScheduleInputSource( manifest, sourcePath ) );
      }

      IPlatformImportBundle platformImportBundle = build( bundleBuilder );
      try {
        importer.importFile( platformImportBundle );
        successfulFilesImportCount++;
        getLogger().debug("Successfully imported repository object with path [ " + repositoryFilePath + " ]");
      } catch (PlatformImportException e) {
        getLogger().error("Error importing repository object with path [ " + repositoryFilePath + " ]. Cause [ " + e.getLocalizedMessage() + " ]");
      }

      if ( bundleInputStream != null ) {
        bundleInputStream.close();
        bundleInputStream = null;
      }
    }
    getLogger().info("Successfully imported [ " + successfulFilesImportCount + " ] out of [ " + files.size() + " ]" );

    // Process locale files.
    getLogger().info( "****************************[ Start: Import Locale File(s) ] **********************************" );
    try {
      localeFilesProcessor.processLocaleFiles( importer );
    } catch (PlatformImportException e) {
      getLogger().error("Error importing locale files. Cause [ " + e.getLocalizedMessage() + " ]");
    } finally {
      getLogger().info( "****************************[ End: Import Locale File(s) ] **********************************" );
    }
    getLogger().info( "*********************** [ End: Import File/Folder(s) ] ***********************************" );
  }

  protected void importJDBCDataSource( ExportManifest manifest ) {
    getLogger().info( "****************************[ Start: Import DataSource(s) ] **********************************" );
    // Add DB Connections
    List<org.pentaho.platform.plugin.services.importexport.exportManifest.bindings.DatabaseConnection> datasourceList = manifest.getDatasourceList();
    if ( datasourceList != null ) {
      int successfulDatasourceImportCount=0;
      getLogger().info( "Found  [ " + datasourceList.size() + " ] DataSource(s)  to import"   );
      IDatasourceMgmtService datasourceMgmtSvc = PentahoSystem.get( IDatasourceMgmtService.class );
      for ( org.pentaho.platform.plugin.services.importexport.exportManifest.bindings.DatabaseConnection databaseConnection : datasourceList ) {
        if ( databaseConnection.getDatabaseType() == null ) {
          // don't try to import the connection if there is no type it will cause an error
          // However, if this is the DI Server, and the connection is defined in a ktr, it will import automatically
          getLogger().warn( Messages.getInstance()
                  .getString( "SolutionImportHandler.ConnectionWithoutDatabaseType", databaseConnection.getName() ) );
          continue;
        }
        try {
          IDatabaseConnection existingDBConnection =
                  datasourceMgmtSvc.getDatasourceByName( databaseConnection.getName() );
          if ( existingDBConnection != null && existingDBConnection.getName() != null ) {
            if ( isOverwriteFile() ) {
              databaseConnection.setId( existingDBConnection.getId() );
              datasourceMgmtSvc.updateDatasourceByName( databaseConnection.getName(),
                      DatabaseConnectionConverter.export2model( databaseConnection ) );
            }
          } else {
            datasourceMgmtSvc.createDatasource( DatabaseConnectionConverter.export2model( databaseConnection ) );
          }
          successfulDatasourceImportCount++;
        } catch ( Exception e ) {
          getLogger().error("Error importing JDBC DataSource [ " + databaseConnection.getName() + " ]. Cause [ " + e.getLocalizedMessage() + " ]");
        }
      }
      getLogger().info("Successfully imported [ " + successfulDatasourceImportCount + " ] out of [ " + datasourceList.size() + " ]" );
    }
    getLogger().info( "****************************[ End: Import DataSource(s) ] **********************************" );
  }

  List<IJob> getAllJobs( ISchedulerResource schedulerResource ) {
    return schedulerResource.getJobsList();
  }

  private RepositoryFile getFile( IPlatformImportBundle importBundle, IRepositoryFileBundle fileBundle ) {
    String repositoryFilePath =
      repositoryPathConcat( importBundle.getPath(), fileBundle.getPath(), fileBundle.getFile().getName() );
    return repository.getFile( repositoryFilePath );
  }

  protected void importSchedules( List<IJobScheduleRequest> scheduleList ) throws PlatformImportException {
    getLogger().info( "*********************** [ Start: Import Schedule(s) ] **************************************" );
    if ( CollectionUtils.isNotEmpty( scheduleList ) ) {
      getLogger().info("Found " + scheduleList.size() + " schedules in the manifest");
      int successfulScheduleImportCount = 0;
      IScheduler scheduler = PentahoSystem.get( IScheduler.class, "IScheduler2", null ); //$NON-NLS-1$
      ISchedulerResource schedulerResource = scheduler.createSchedulerResource();
      getLogger().debug("Pausing the scheduler before the start of the import process");
      schedulerResource.pause();
      getLogger().debug("Successfully paused the scheduler");
      for ( IJobScheduleRequest jobScheduleRequest : scheduleList ) {
        getLogger().debug("Importing schedule name [ " + jobScheduleRequest.getJobName() + "] inputFile [ " + jobScheduleRequest.getInputFile() + " ] outputFile [ " + jobScheduleRequest.getOutputFile() + "]");
        boolean jobExists = false;

        List<IJob> jobs = getAllJobs( schedulerResource );
        if ( jobs != null ) {

          //paramRequest to map<String, Serializable>
          Map<String, Serializable> mapParamsRequest = new HashMap<>();
          for ( IJobScheduleParam paramRequest : jobScheduleRequest.getJobParameters() ) {
            mapParamsRequest.put( paramRequest.getName(), paramRequest.getValue() );
          }

          // We will check the existing job in the repository. If the job being imported exists, we will remove it from the repository
          for ( IJob job : jobs ) {

            if ( ( mapParamsRequest.get( RESERVEDMAPKEY_LINEAGE_ID ) != null )
              && ( mapParamsRequest.get( RESERVEDMAPKEY_LINEAGE_ID )
              .equals( job.getJobParams().get( RESERVEDMAPKEY_LINEAGE_ID ) ) ) ) {
              jobExists = true;
            }

            if ( overwriteFile && jobExists ) {
              getLogger().debug("Schedule  [ " + jobScheduleRequest.getJobName() + "] already exists and overwrite flag is set to true. Removing the job so we can add it again");
              IJobRequest jobRequest = scheduler.createJobRequest();
              jobRequest.setJobId( job.getJobId() );
              schedulerResource.removeJob( jobRequest );
              jobExists = false;
              break;
            }
          }
        }

        if ( !jobExists ) {
          try {
            Response response = createSchedulerJob( schedulerResource, jobScheduleRequest );
            if ( response.getStatus() == Response.Status.OK.getStatusCode() ) {
              if ( response.getEntity() != null ) {
                // get the schedule job id from the response and add it to the import session
                ImportSession.getSession().addImportedScheduleJobId( response.getEntity().toString() );
                getLogger().debug("Successfully imported schedule [ " + jobScheduleRequest.getJobName() + " ] ");
                successfulScheduleImportCount++;
              }
            } else {
              getLogger().error("Unable to create schedule [ " + jobScheduleRequest.getJobName() + " ] cause [ " + response.getEntity().toString() + " ]");
            }
          } catch ( Exception e ) {
            // there is a scenario where if the file scheduled has a space in the file name, that it won't work. the
            // di server

            // replaces spaces with underscores and the export mechanism can't determine if it needs this to happen
            // or not
            // so, if we failed to import and there is a space in the path, try again but this time with replacing
            // the space(s)
            if ( jobScheduleRequest.getInputFile().contains( " " ) || jobScheduleRequest.getOutputFile()
              .contains( " " ) ) {
              getLogger().debug( Messages.getInstance()
                .getString( "SolutionImportHandler.SchedulesWithSpaces", jobScheduleRequest.getInputFile() ) );
              File inFile = new File( jobScheduleRequest.getInputFile() );
              File outFile = new File( jobScheduleRequest.getOutputFile() );
              String inputFileName = inFile.getParent() + RepositoryFile.SEPARATOR
                + inFile.getName().replace( " ", "_" );
              String outputFileName = outFile.getParent() + RepositoryFile.SEPARATOR
                + outFile.getName().replace( " ", "_" );
              jobScheduleRequest.setInputFile( inputFileName );
              jobScheduleRequest.setOutputFile( outputFileName );
              try {
                if ( !File.separator.equals( RepositoryFile.SEPARATOR ) ) {
                  // on windows systems, the backslashes will result in the file not being found in the repository
                  jobScheduleRequest.setInputFile( inputFileName.replace( File.separator, RepositoryFile.SEPARATOR ) );
                  jobScheduleRequest
                    .setOutputFile( outputFileName.replace( File.separator, RepositoryFile.SEPARATOR ) );
                }
                Response response = createSchedulerJob( schedulerResource, jobScheduleRequest );
                if ( response.getStatus() == Response.Status.OK.getStatusCode() ) {
                  if ( response.getEntity() != null ) {
                    // get the schedule job id from the response and add it to the import session
                    ImportSession.getSession().addImportedScheduleJobId( response.getEntity().toString() );
                    successfulScheduleImportCount++;
                  }
                }
              } catch ( Exception ex ) {
                // log it and keep going. we should stop processing all schedules just because one fails.
                getLogger().error( Messages.getInstance()
                  .getString( "SolutionImportHandler.ERROR_0001_ERROR_CREATING_SCHEDULE", e.getMessage() ), ex );
              }
            } else {
              // log it and keep going. we should stop processing all schedules just because one fails.
              getLogger().error( Messages.getInstance()
                .getString( "SolutionImportHandler.ERROR_0001_ERROR_CREATING_SCHEDULE", e.getMessage() ) );
            }
          }
        } else {
          getLogger().info( Messages.getInstance()
            .getString( "DefaultImportHandler.ERROR_0009_OVERWRITE_CONTENT", jobScheduleRequest.toString() ) );
        }
      }
      getLogger().info("Successfully imported [ " + successfulScheduleImportCount + " ] out of [ " + scheduleList.size() + " ]" );
      schedulerResource.start();
      getLogger().debug("Successfully started the scheduler");
    }
    getLogger().info( "*********************** [ End: Import Schedule(s) ] **************************************" );
  }

  protected void importMetaStore( ExportManifestMetaStore manifestMetaStore, boolean overwrite ) {
    getLogger().info( "********************** [ Start: Import MetaStore ] ******************************************" );
    if ( manifestMetaStore != null ) {
      // get the zipped metastore from the export bundle
      RepositoryFileImportBundle.Builder bundleBuilder =
        new RepositoryFileImportBundle.Builder()
          .path( manifestMetaStore.getFile() )
          .name( manifestMetaStore.getName() )
          .withParam( "description", manifestMetaStore.getDescription() )
          .charSet( UTF_8 )
          .overwriteFile( overwrite )
          .mime( "application/vnd.pentaho.metastore" );

      cachedImports.put( manifestMetaStore.getFile(), bundleBuilder );
      getLogger().info( "Successfully imported metastore" );
    }
    getLogger().info( "********************** [ End: Import MetaStore ] ******************************************" );
  }

  /**
   * Imports UserExport objects into the platform as users.
   *
   * @param users
   * @return A map of role names to list of users in that role
   */
  protected Map<String, List<String>> importUsers( List<UserExport> users ) {
    Map<String, List<String>> roleToUserMap = new HashMap<>();
    IUserRoleDao roleDao = PentahoSystem.get( IUserRoleDao.class );
    ITenant tenant = new Tenant( "/pentaho/" + TenantUtils.getDefaultTenant(), true );
    int successFullUserImportCount = 0;
    getLogger().info("****************************** [Start Import User(s)] ***************************");
    if ( users != null && roleDao != null ) {
      getLogger().info("Found [ " + users.size() + " ] users to import");
      for ( UserExport user : users ) {
        String password = user.getPassword();
        getLogger().debug( Messages.getInstance().getString( "USER.importing", user.getUsername() ) );

        // map the user to the roles he/she is in
        for ( String role : user.getRoles() ) {
          List<String> userList;
          if ( !roleToUserMap.containsKey( role ) ) {
            userList = new ArrayList<>();
            roleToUserMap.put( role, userList );
          } else {
            userList = roleToUserMap.get( role );
          }
          userList.add( user.getUsername() );
        }

        String[] userRoles = user.getRoles().toArray( new String[] {} );
        try {
          getLogger().debug( "Importing user [ " + user.getUsername() + " ] " );
          roleDao.createUser( tenant, user.getUsername(), password, null, userRoles );
          getLogger().debug( "Successfully imported user [ " + user.getUsername() + " ]" );
          successFullUserImportCount++;
        } catch ( AlreadyExistsException e ) {
          // it's ok if the user already exists, it is probably a default user
          getLogger().info( Messages.getInstance().getString( "USER.Already.Exists", user.getUsername() ) );

          try {
            if ( isOverwriteFile() ) {
              getLogger().debug( "Overwrite is set to true. So reImporting user [ " + user.getUsername() + " ]" );
              // set the roles, maybe they changed
              roleDao.setUserRoles( tenant, user.getUsername(), userRoles );

              // set the password just in case it changed
              roleDao.setPassword( tenant, user.getUsername(), password );
              successFullUserImportCount++;
            }
          } catch ( Exception ex ) {
            // couldn't set the roles or password either
            getLogger().warn( Messages.getInstance()
                    .getString( "ERROR.OverridingExistingUser", user.getUsername() ) );
            getLogger().debug( Messages.getInstance()
              .getString( "ERROR.OverridingExistingUser", user.getUsername() ), ex );
          }
        } catch ( Exception e ) {
          getLogger().debug( Messages.getInstance()
            .getString( "ERROR.OverridingExistingUser", user.getUsername() ), e );
          getLogger().error( Messages.getInstance()
                  .getString( "ERROR.OverridingExistingUser", user.getUsername() ) );
        }
        getLogger().debug( "Importing user [ " + user.getUsername() + " ] specific settings" );
        importUserSettings( user );
        getLogger().debug( "Successfully imported user [ " + user.getUsername() + " ] specific settings" );
      }
    }
    getLogger().info("Successfully imported [ " + successFullUserImportCount + " ] out of [ " + users.size() + " ]" );
    getLogger().info("****************************** [End Import User(s)] ***************************");
    return roleToUserMap;
  }

  protected void importGlobalUserSettings( List<ExportManifestUserSetting> globalSettings ) {
    getLogger().debug( "************************[ Start: Import global user  settings] *************************" );
    IUserSettingService settingService = PentahoSystem.get( IUserSettingService.class );
    if ( settingService != null ) {
      for ( ExportManifestUserSetting globalSetting : globalSettings ) {
        if ( isOverwriteFile() ) {
          getLogger().trace( "Overwrite flag is set to true.");
          settingService.setGlobalUserSetting( globalSetting.getName(), globalSetting.getValue() );
          getLogger().debug( "Finished import of global user setting with name [ " + globalSetting.getName() + " ]" );
        } else {
          getLogger().trace( "Overwrite flag is set to false.");
          IUserSetting userSetting = settingService.getGlobalUserSetting( globalSetting.getName(), null );
          if ( userSetting == null ) {
            settingService.setGlobalUserSetting( globalSetting.getName(), globalSetting.getValue() );
            getLogger().debug( "Finished import of global user setting with name [ " + globalSetting.getName() + " ]" );
          }
        }
      }
    }
    getLogger().debug( "************************[ End: Import global user settings] *************************" );
  }

  protected void importUserSettings( UserExport user ) {
    IUserSettingService settingService = PentahoSystem.get( IUserSettingService.class );
    IAnyUserSettingService userSettingService = null;
    int userSettingsListSize = 0;
    int successfulUserSettingsImportCount = 0;
    if ( settingService != null && settingService instanceof IAnyUserSettingService ) {
      userSettingService = (IAnyUserSettingService) settingService;
    }
    getLogger().info( "************************[ Start: Import user specific settings] *************************" );

    if ( userSettingService != null ) {
      List<ExportManifestUserSetting> exportedSettings = user.getUserSettings();
      userSettingsListSize = user.getUserSettings().size();
      getLogger().info( "Found  [ " + userSettingsListSize + " ] user specific settings for user [" + user.getUsername() + " ]" );
      try {
        for ( ExportManifestUserSetting exportedSetting : exportedSettings ) {
          getLogger().debug( "Importing user specific setting  [ " + exportedSetting.getName() + " ]"   );
          if ( isOverwriteFile() ) {
            getLogger().debug( "Overwrite is set to true. So reImporting setting  [ " + exportedSetting.getName() + " ]"   );
            userSettingService.setUserSetting( user.getUsername(),
              exportedSetting.getName(), exportedSetting.getValue() );
            getLogger().debug( "Finished import of user specific setting with name [ " + exportedSetting.getName() + " ]" );
          } else {
            // see if it's there first before we set this setting
            getLogger().debug( "Overwrite is set to false. Only import setting  [ " + exportedSetting.getName() + " ] if is does not exist"   );
            IUserSetting userSetting =
              userSettingService.getUserSetting( user.getUsername(), exportedSetting.getName(), null );
            if ( userSetting == null ) {
              // only set it if we didn't find that it exists already
              userSettingService.setUserSetting( user.getUsername(), exportedSetting.getName(), exportedSetting.getValue() );
              getLogger().debug( "Finished import of user specific setting with name [ " + exportedSetting.getName() + " ]" );
            }
          }
          successfulUserSettingsImportCount++;
          getLogger().debug( "Successfully imported setting  [ " + exportedSetting.getName() + " ]"   );
        }
      } catch ( SecurityException e ) {
        String errorMsg = Messages.getInstance().getString( "ERROR.ImportingUserSetting", user.getUsername() );
        getLogger().error( errorMsg );
        getLogger().debug( errorMsg, e );
      } finally {
        getLogger().info("Successfully imported [ " + successfulUserSettingsImportCount + " ]" +
                " out of [ " +  userSettingsListSize + " ] user specific settings");
        getLogger().info( "************************[ End: Import user specific settings] *************************" );
      }
    }
  }

  protected void importRoles( List<RoleExport> roles, Map<String, List<String>> roleToUserMap ) {
    getLogger().info( "*********************** [ Start: Import Role(s) ] ***************************************" );
    if ( roles != null ) {
      IUserRoleDao roleDao = PentahoSystem.get( IUserRoleDao.class );
      ITenant tenant = new Tenant( "/pentaho/" + TenantUtils.getDefaultTenant(), true );
      IRoleAuthorizationPolicyRoleBindingDao roleBindingDao = PentahoSystem.get(
        IRoleAuthorizationPolicyRoleBindingDao.class );

      Set<String> existingRoles = new HashSet<>();
      getLogger().info( "Found  [ " + roles.size() + " ] roles to import"   );
      int successFullRoleImportCount = 0;
      for ( RoleExport role : roles ) {
        getLogger().debug( Messages.getInstance().getString( "ROLE.importing", role.getRolename() ) );
        try {
          List<String> users = roleToUserMap.get( role.getRolename() );
          String[] userarray = users == null ? new String[] {} : users.toArray( new String[] {} );
          IPentahoRole role1 = roleDao.createRole( tenant, role.getRolename(), null, userarray );
          successFullRoleImportCount++;
        } catch ( AlreadyExistsException e ) {
          existingRoles.add( role.getRolename() );
          // it's ok if the role already exists, it is probably a default role
          getLogger().info( Messages.getInstance().getString( "ROLE.Already.Exists", role.getRolename() ) );
        }
        try {
          if ( existingRoles.contains( role.getRolename() ) ) {
            //Only update an existing role if the overwrite flag is set
            if ( isOverwriteFile() ) {
              getLogger().debug( "Overwrite is set to true so reImporting role [ " +  role.getRolename() + "]" );
              roleBindingDao.setRoleBindings( tenant, role.getRolename(), role.getPermissions() );
            }
          } else {
            getLogger().debug( "Updating the role mapping from runtime roles to logical roles for  [ " +  role.getRolename() + "]" );
            //Always write a roles permissions that were not previously existing
            roleBindingDao.setRoleBindings( tenant, role.getRolename(), role.getPermissions() );
          }
          successFullRoleImportCount++;
        } catch ( Exception e ) {
          getLogger().info( Messages.getInstance()
            .getString( "ERROR.SettingRolePermissions", role.getRolename() ), e );
        }
      }
      getLogger().info("Successfully imported [ " + successFullRoleImportCount + " ] out of [ " + roles.size() + " ]" );
    }
    getLogger().info( "*********************** [ End: Import Role(s) ] ***************************************" );

  }

  /**
   * <p>Import the Metadata</p>
   *
   * @param metadataList metadata to be imported
   * @param preserveDsw  whether or not to preserve DSW settings
   */
  protected void importMetadata( List<ExportManifestMetadata> metadataList, boolean preserveDsw ) {
    getLogger().info( "*********************** [ Start: Import Metadata DataSource(s)  ] *****************************" );
    if ( null != metadataList ) {
      int successfulMetadataModelImport = 0;
      getLogger().info( "Found  [ " +  metadataList.size() + "] metadata models to import" );
      for ( ExportManifestMetadata exportManifestMetadata : metadataList ) {
        getLogger().debug( "Importing  [ " +  exportManifestMetadata.getDomainId() + " ] model" );
        String domainId = exportManifestMetadata.getDomainId();
        if ( domainId != null && !domainId.endsWith( XMI_EXTENSION ) ) {
          domainId = domainId + XMI_EXTENSION;
        }
        RepositoryFileImportBundle.Builder bundleBuilder =
          new RepositoryFileImportBundle.Builder().charSet( UTF_8 )
            .hidden( RepositoryFile.HIDDEN_BY_DEFAULT ).schedulable( RepositoryFile.SCHEDULABLE_BY_DEFAULT )
            // let the parent bundle control whether or not to preserve DSW settings
            .preserveDsw( preserveDsw )
            .overwriteFile( isOverwriteFile() )
            .mime( "text/xmi+xml" )
            .withParam( DOMAIN_ID, domainId );

        cachedImports.put( exportManifestMetadata.getFile(), bundleBuilder );
        getLogger().info( " Successfully Imported  [ " +  exportManifestMetadata.getDomainId() + " ] model" );
        successfulMetadataModelImport++;
      }
      getLogger().info("Successfully imported [ " + successfulMetadataModelImport + " ] out of [ " + metadataList.size() + " ] metadata models" );
    }
    getLogger().info( "*********************** [ End: Import Metadata DataSource(s)  ] *****************************" );
  }

  protected void importMondrian( List<ExportManifestMondrian> mondrianList ) {
    getLogger().info( "*********************** [ Start: Import Mondrian DataSource(s)  ] *****************************" );

    if ( null != mondrianList ) {
      int successfulMondrianSchemaImport = 0;
      getLogger().info( "Found  [ " +  mondrianList.size() + " ] mondrian schemas to import" );
      for ( ExportManifestMondrian exportManifestMondrian : mondrianList ) {
        getLogger().debug( "Importing  [ " +  exportManifestMondrian.getCatalogName() + " ] olap model" );
        String catName = exportManifestMondrian.getCatalogName();
        Parameters parametersMap = exportManifestMondrian.getParameters();
        StringBuilder parametersStr = new StringBuilder();
        for ( Map.Entry<String, String> e : parametersMap.entrySet() ) {
          parametersStr.append( e.getKey() ).append( '=' ).append( e.getValue() ).append( ';' );
        }

        RepositoryFileImportBundle.Builder bundleBuilder =
          new RepositoryFileImportBundle.Builder().charSet( UTF_8 ).hidden( RepositoryFile.HIDDEN_BY_DEFAULT )
            .schedulable( RepositoryFile.SCHEDULABLE_BY_DEFAULT ).name( catName ).overwriteFile(
              isOverwriteFile() ).mime( "application/vnd.pentaho.mondrian+xml" )
            .withParam( "parameters", parametersStr.toString() )
            .withParam( DOMAIN_ID, catName ); // TODO: this is definitely named wrong at the very least.
        // pass as param if not in parameters string
        String xmlaEnabled = "" + exportManifestMondrian.isXmlaEnabled();
        bundleBuilder.withParam( "EnableXmla", xmlaEnabled );

        cachedImports.put( exportManifestMondrian.getFile(), bundleBuilder );

        String annotationsFile = exportManifestMondrian.getAnnotationsFile();
        if ( annotationsFile != null ) {
          RepositoryFileImportBundle.Builder annotationsBundle =
            new RepositoryFileImportBundle.Builder().path( MondrianCatalogRepositoryHelper.ETC_MONDRIAN_JCR_FOLDER
              + RepositoryFile.SEPARATOR + catName ).name( "annotations.xml" ).charSet( UTF_8 ).overwriteFile(
              isOverwriteFile() ).mime( "text/xml" ).hidden( RepositoryFile.HIDDEN_BY_DEFAULT ).schedulable(
              RepositoryFile.SCHEDULABLE_BY_DEFAULT ).withParam( DOMAIN_ID, catName );
          cachedImports.put( annotationsFile, annotationsBundle );
        }
        successfulMondrianSchemaImport++;
        getLogger().debug( " Successfully Imported  [ " +  exportManifestMondrian.getCatalogName() + " ] mondrian schema" );
      }
      getLogger().info("Successfully imported [ " + successfulMondrianSchemaImport + " ] out of [ " + mondrianList.size() + " ]" );
    }
    getLogger().info( "*********************** [ End: Import Mondrian DataSource(s)  ] *****************************" );
  }

  /**
   * See BISERVER-13481 . For backward compatibility we must check if there are any schedules
   * which refers to this file. If yes make this file schedulable
   */
  @VisibleForTesting
  boolean fileIsScheduleInputSource( ExportManifest manifest, String sourcePath ) {
    boolean isSchedulable = false;
    if ( sourcePath != null && manifest != null
      && manifest.getScheduleList() != null ) {
      String path = sourcePath.startsWith( "/" ) ? sourcePath : "/" + sourcePath;
      isSchedulable = manifest.getScheduleList().stream()
        .anyMatch( schedule -> path.equals( schedule.getInputFile() ) );
    }

    if ( isSchedulable ) {
      getLogger().warn( Messages.getInstance()
        .getString( "ERROR.ScheduledWithoutPermission", sourcePath ) );
      getLogger().warn( Messages.getInstance().getString( "SCHEDULE.AssigningPermission", sourcePath ) );
    }

    return isSchedulable;
  }

  @VisibleForTesting
  protected boolean isFileHidden( RepositoryFile file, ManifestFile manifestFile, String sourcePath ) {
    Boolean result = manifestFile.isFileHidden();
    if ( result != null ) {
      return result; // file absent or must receive a new setting and the setting is exist
    }
    if ( file != null ) {
      return file.isHidden(); // old setting
    }
    if ( solutionHelper.isInHiddenList( sourcePath ) ) {
      return true;
    }
    return RepositoryFile.HIDDEN_BY_DEFAULT; // default setting of type
  }

  @VisibleForTesting
  protected boolean isSchedulable( RepositoryFile file, ManifestFile manifestFile ) {
    Boolean result = manifestFile.isFileSchedulable();
    if ( result != null ) {
      return result; // file absent or must receive a new setting and the setting is exist
    }
    if ( file != null ) {
      return file.isSchedulable(); // old setting
    }
    return RepositoryFile.SCHEDULABLE_BY_DEFAULT; // default setting of type
  }

  private String repositoryPathConcat( String path, String... subPaths ) {
    for ( String subPath : subPaths ) {
      path = RepositoryFilenameUtils.concat( path, subPath );
    }
    return path;
  }

  private boolean processZip( InputStream inputStream ) {
    this.files = new ArrayList<>();
    getLogger().info( "****************************** [ Start: Import Repository File/Folder(s) ] **********************************" );
    try ( ZipInputStream zipInputStream = new ZipInputStream( inputStream ) ) {
      FileService fileService = new FileService();
      ZipEntry entry = zipInputStream.getNextEntry();
      while ( entry != null ) {
        final String entryName = RepositoryFilenameUtils.separatorsToRepository( entry.getName() );
        getLogger().debug( Messages.getInstance().getString( "ZIPFILE.ProcessingEntry", entryName ) );
        final String decodedEntryName = ExportFileNameEncoder.decodeZipFileName( entryName );
        File tempFile = null;
        boolean isDir = entry.isDirectory();
        if ( !isDir ) {
          if ( !solutionHelper.isInApprovedExtensionList( entryName ) ) {
            zipInputStream.closeEntry();
            entry = zipInputStream.getNextEntry();
            continue;
          }

          if ( !fileService.isValidFileName( decodedEntryName ) ) {
            getLogger().debug( "This not a valid file name. Failing the import" );
            throw new PlatformImportException(
              Messages.getInstance().getString( "DefaultImportHandler.ERROR_0011_INVALID_FILE_NAME",
                entryName ), PlatformImportException.PUBLISH_PROHIBITED_SYMBOLS_ERROR );
          }

          tempFile = File.createTempFile( "zip", null );
          tempFile.deleteOnExit();
          try ( FileOutputStream fos = new FileOutputStream( tempFile ) ) {
            IOUtils.copy( zipInputStream, fos );
          }
        } else {
          if ( !fileService.isValidFileName( decodedEntryName ) ) {
            getLogger().error( "This not a valid file name. Failing the import" );
            throw new PlatformImportException(
              Messages.getInstance().getString( "DefaultImportHandler.ERROR_0012_INVALID_FOLDER_NAME",
                entryName ), PlatformImportException.PUBLISH_PROHIBITED_SYMBOLS_ERROR );
          }
        }
        File file = new File( entryName );
        RepositoryFile repoFile =
          new RepositoryFile.Builder( file.getName() ).folder( isDir ).hidden( false ).build();
        String parentDir =
          file.getParent() == null ? RepositoryFile.SEPARATOR : file.getParent()
            + RepositoryFile.SEPARATOR;
        IRepositoryFileBundle repoFileBundle =
          new RepositoryFileBundle( repoFile, null, parentDir, tempFile, UTF_8, null );

        if ( EXPORT_MANIFEST_XML_FILE.equals( file.getName() ) ) {
          initializeAclManifest( repoFileBundle );
        } else {
          getLogger().debug( "Adding file " + repoFile.getName() + " to list for later processing " );
          files.add( repoFileBundle );
        }
        zipInputStream.closeEntry();
        entry = zipInputStream.getNextEntry();
      }
    } catch ( IOException | PlatformImportException e ) {
      getLogger().error( Messages.getInstance()
        .getErrorString( "ZIPFILE.ExceptionOccurred", e.getLocalizedMessage() ), e );
      return false;
    }
    getLogger().info( "****************************** [ End: Import Repository File/Folder(s) ] **********************************" );
    return true;
  }

  private void initializeAclManifest( IRepositoryFileBundle file ) {
    try {
      byte[] bytes = IOUtils.toByteArray( file.getInputStream() );
      ByteArrayInputStream in = new ByteArrayInputStream( bytes );
      getImportSession().setManifest( ExportManifest.fromXml( in ) );
    } catch ( Exception e ) {
      getLogger().trace( e );
    }
  }

  @Override
  public List<IMimeType> getMimeTypes() {
    return mimeTypes;
  }

  // handlers that extend this class may override this method and perform operations
  // over the bundle prior to entering its designated importer.importFile()
  public IPlatformImportBundle build( RepositoryFileImportBundle.Builder builder ) {
    return builder != null ? builder.build() : null;
  }

  // handlers that extend this class may override this method and perform operations
  // over the job prior to its creation at scheduler.createJob()
  public Response createSchedulerJob( ISchedulerResource scheduler, IJobScheduleRequest jobScheduleRequest )
    throws IOException {
    Response rs = scheduler != null ? (Response) scheduler.createJob( jobScheduleRequest ) : null;
    if ( jobScheduleRequest.getJobState() != JobState.NORMAL ) {
      IJobRequest jobRequest = PentahoSystem.get( IScheduler.class, "IScheduler2", null ).createJobRequest();
      jobRequest.setJobId( rs.getEntity().toString() );
      scheduler.pauseJob( jobRequest );
    }
    return rs;
  }

  public boolean isOverwriteFile() {
    return overwriteFile;
  }

  public void setOverwriteFile( boolean overwriteFile ) {
    this.overwriteFile = overwriteFile;
  }
}
