#!/usr/bin/perl
use utf8;
use Parallel::Fork::BossWorkerAsync;
use DBI;

#use DBD::mysql;
use DBD::Pg;
require LWP::UserAgent;
use XML::XPath;
use XML::XPath::XMLParser;
use Encode;
use URI::Escape;
use POSIX;
use Log::Log4perl;
use Try::Tiny;
use JSON;

package Master;

sub new {
	my $class = shift;
	my $self  = {
		id           => shift,
		title        => shift,
		main_release => shift,
		year         => shift,
		genres       => shift,
		styles       => shift,
		releaselist     => shift
	};
	bless $self, $class;
	return $self;
}
sub TO_JSON { return { %{ shift() } }; }
1;

package Release;

sub new {
	my $class = shift;
	my $self  = {
		release_id     => shift,
		title          => shift,
		country        => shift,
		released_date  => shift,
		genres         => shift,
		styles         => shift,
		master_id      => shift,
		releaseartists => shift,

		#artist_id     => shift,
		#artist_name   => shift,
		#anv           => shift,
		#join_relation => shift,
		#position      => shift,
		tracks => undef,
		formats => undef,
	};
	bless $self, $class;
	return $self;
}
sub TO_JSON { return { %{ shift() } }; }

sub setReleaseId {
	my ( $self, $releaseId ) = @_;
	$self->{release_id} = $releaseId if defined($releaseId);
	return $self->{release_id};
}

sub getReleaseId {
	my ($self) = @_;
	return $self->{release_id};
}

sub setTitle {
	my ( $self, $title ) = @_;
	$self->{title} = $title if defined($title);
	return $self->{title};
}

sub getTitle {
	my ($self) = @_;
	return $self->{title};
}

sub setCountry {
	my ( $self, $country ) = @_;
	$self->{country} = $country if defined($country);
	return $self->{country};
}

sub getCountry {
	my ($self) = @_;
	return $self->{country};
}

sub setReleasedDate {
	my ( $self, $releasedDate ) = @_;
	$self->{released_date} = $releasedDate if defined($releasedDate);
	return $self->{released_date};
}

sub getReleasedDate {
	my ($self) = @_;
	return $self->{released_date};
}
1;

package Track;

sub new {
	my $class = shift;
	my $self  = {
		release_id => shift,
		position =>shift,
		track_id   => shift,
		title      => shift,
		duration   => shift,
		trackno    => shift,
		artists    => shift,
		extraartists =>shift,

	};
	bless $self, $class;
	return $self;
}
sub TO_JSON { return { %{ shift() } }; }
1;

package TrackArtists;

sub new {
	my $class = shift;
	my $self  = {
		track_id      => shift,
		position      => shift,
		artist_name   => shift,
		artist_id     => shift,
		anv           => shift,
		join_relation => shift,
		artistdetails => shift,

	};
	bless $self, $class;
	return $self;
}
sub TO_JSON { return { %{ shift() } }; }
1;

package TrackExtraArtists;

sub new {
	my $class = shift;
	my $self  = {
		track_id      => shift,
		artist_id     => shift,
		artist_name   => shift,
		anv           => shift,
		role 		  => shift,
		artistdetails => shift,
	};
	bless $self, $class;
	return $self;
}
sub TO_JSON { return { %{ shift() } }; }
1;

package ReleaseArtists;

sub new {
	my $class = shift;
	my $self  = {
		release_id    => shift,
		position      => shift,
		artist_name   => shift,
		artist_id     => shift,
		anv           => shift,
		join_relation => shift,
		artistdetails => shift,
	};
	bless $self, $class;
	return $self;
}
sub TO_JSON { return { %{ shift() } }; }
1;

package ArtistDetails;

sub new {
	my $class = shift;
	my $self  = {
		artist_id    => shift,
		name      => shift,
		real_name   => shift,
		urls     => shift,
		namevariations           => shift,
		aliases => shift,
		profile => shift,
		members => shift,
		members_id => shift,

	};
	bless $self, $class;
	return $self;
}
sub TO_JSON { return { %{ shift() } }; }
1;

package Format;

sub new {
	my $class = shift;
	my $self  = {
		release_id => shift,
		position =>shift,
		format_name   => shift,
		qty      => shift,
		descriptions   => shift,
	};
	bless $self, $class;
	return $self;
}
sub TO_JSON { return { %{ shift() } }; }
1;

# Here is the main program using above classes.
package main;
#log4perl.appender.LOG1.filename  = /aurora.cs/local7/apo/DiscogsExtracterlog.log
use constant LOG_SETTING => q(
   log4perl.rootLogger              = DEBUG, LOG1, SCREEN
   log4perl.appender.SCREEN         = Log::Log4perl::Appender::Screen
   log4perl.appender.SCREEN.stderr  = 0
   log4perl.appender.SCREEN.layout  = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.SCREEN.layout.ConversionPattern = %m %n
   log4perl.appender.LOG1           = Log::Log4perl::Appender::File
   
   log4perl.appender.LOG1.filename  = /aurora.cs/ssd/apo/DiscogsExtracterlog.log
   log4perl.appender.LOG1.mode      = append
   log4perl.appender.LOG1.layout    = Log::Log4perl::Layout::PatternLayout
   log4perl.appender.LOG1.layout.ConversionPattern = %d %p %m %n
);
use constant OUTPUT_DIR     => "/aurora.cs/ssd/apo/discogs/discogs_output41";
use constant CONNECTION_STR =>
  "DBI:Pg:database=discogs50;host=localhost;port=5432";
use constant DBUSER => "gjamuar";

$SIG{'INT'}    = 'SIG_handler';
$SIG{'ABRT2'}  = 'SIG_handler';
$SIG{'BREAK2'} = 'SIG_handler';
$SIG{'TERM2'}  = 'SIG_handler';
$SIG{'SEGV2'}  = 'SIG_handler';
$SIG{'FPE2'}   = 'SIG_handler';
$SIG{'ILL2'}   = 'SIG_handler';

$SIG{__WARN__} = 'WARN_handler';

$SIG{__DIE__} = 'DIE_handler';

sub WARN_handler {

	my ($signal) = @_;

	sendToLogfile("WARN: $signal");

}

sub DIE_handler {

	my ($signal) = @_;

	sendToLogfile("DIE: $signal");

}

sub SIG_handler {

	my ($signal) = @_;

	sendToLogfile("WARN: $signal");

}

my $log_conf = LOG_SETTING;
Log::Log4perl::init( \$log_conf );

my $logger = Log::Log4perl->get_logger();

#to run just one
#aurora% perl discogsextractor.pl tempoutput 0 1 1 48919
#aurora% perl discogsextractor.pl outputdir 0 1 1 artistid

my $runone      = 0;
my $start       = 0;
my $set_num     = 30000;                      #1;
my $workercount = 70;                         #2
my $rows        = $set_num * $workercount;    #3;
if ( $runone eq 1 ) {
	$set_num     = 1;
	$workercount = 1;
	$rows        = 3;
}

my $dirpath = OUTPUT_DIR;
my $argnum  = $#ARGV;

if ( $argnum eq 0 ) {

	$dirpath = shift @ARGV;

	#print  "$dirpath\n";
	#break;
}
elsif ( $argnum eq 1 ) {
	$dirpath = shift @ARGV;
	$start   = shift @ARGV;
	print $ARGV[1];

	#break;
}
elsif ( $argnum eq 2 ) {
	$dirpath = shift @ARGV;
	$start   = shift @ARGV;
	$set_num = shift @ARGV;

	#break;
}
elsif ( $argnum eq 4 ) {
	$dirpath     = shift @ARGV;
	$start       = shift @ARGV;
	$set_num     = shift @ARGV;
	$runone      = shift @ARGV;
	$set_num     = 1;
	$workercount = 1;
	$rows        = 3;
	$logdata     = shift @ARGV;

	#break;
}

unless ( -d $dirpath ) {
	mkdir $dirpath, 0755 or die "Cannot create directory for $dirpath.:$1 $!";
}
chdir $dirpath;

#logging time in a file
my $epoc = time();
my $success0 = open TIMELOG, ">:utf8", "timelog.txt" or die $!;;
print TIMELOG $epoc; 
close TIMELOG;
 
#$artistid = '194';

my $bw = Parallel::Fork::BossWorkerAsync->new(
	work_handler => \&findArtistData,
	worker_count => $workercount,
);

my $db = DBI->connect( CONNECTION_STR, DBUSER, '', { 'RaiseError' => 1 } );

$db->{'pg_enable_utf8'} = 1;

$db->do("SET NAMES 'utf8'");
print "database connected for process  $$\n";

my $stmt_artist_ids =
  $db->prepare(
"Select distinct releases_artists.artist_id from   discogs.releases_artists order by releases_artists.artist_id;"
  );

my @hashlist;
my $count  = 0;
my $count2 = 0;
my $idstr  = undef;
if ( $runone ne 1 ) {
	$stmt_artist_ids->execute();
	while ( my $ref = $stmt_artist_ids->fetchrow_hashref() ) {
		my $id = $ref->{'artist_id'};

		next if ( $id eq '194' );

		#	unshift(@hashlist,$ref->{'artist_id'});
		if ( $count < $set_num ) {
			$idstr = $idstr . ":" . $id;

			#$count++;
		}
		else {
			push @hashlist, { "artist_id" => $idstr, "flag" => 1 };
			$idstr = '';
			$count = 0;
			$idstr = $id;

		}
		$count++;
		$count2++;
	}

	if ( $count gt 0 ) {

		#	$logger->info("Putting one more set of data");
		push @hashlist, { "artist_id" => $idstr };
	}
}
elsif ( $runone eq 1 ) {
	print "Dataset $logdata\n";
	push @hashlist, { "logdata" => $logdata, "flag" => 0 };
}

$logger->info("Process $$: main process,totals artis id = $count2.");

$stmt_artist_ids->finish();

$bw->add_work(@hashlist);

#=for comment
while ( $bw->pending() ) {

	#my $ref = $bw->get_result();
	my $ref = $bw->get_result_nb();

	#  print "inside while\n";

}

#=cut
$db->disconnect();
$bw->shut_down();

#my $stmt_id = $db->prepare("Select release_id from discogs.releases_artists where artist_id = '194' and position =1;");

sub findArtistData {
	my $log_conf = LOG_SETTING;
	Log::Log4perl::init( \$log_conf );

	my $logger = Log::Log4perl->get_logger();
##End Logging settings#####

	#change following line to manually run
	my @idary;
	if ( $_[0]{"flag"} eq 1 ) {
		@idary = split( /:/, $_[0]{"artist_id"} );
	}
	else {
		@idary = split( /:/, $_[0]{"logdata"} );
	}

	my $db = DBI->connect( CONNECTION_STR, DBUSER, '', { 'RaiseError' => 1 } );

	$db->{'pg_enable_utf8'} = 1;

	$db->do("SET NAMES 'utf8'");

	#$logger->info("Starting Process $$: @idary");

	while ( scalar(@idary) gt 0 ) {
		my $artistid = pop @idary;

		if ( $artistid ne undef ) {
			chdir(OUTPUT_DIR);
			$logger->info("Process $$: Artist ID $artistid");

			#print "\n$id  in process  $$\n";
			my $maindir = ceil( $artistid / 30000 );
			unless ( -d $maindir ) {

	   #mkdir $maindir , 0755  or die "Cannot create directory for $id. :$1 $!";
				unless ( mkdir( $maindir, 0755 ) ) {
					$logger->info(
"Process $$: ID $artistid ,Cannot create directory for $artistid."
					);

					#chdir("..");
					next;
				}
			}
			chdir $maindir;

			my $stmt_master = $db->prepare(
				"SELECT 
  master.id, 
  master.title, 
  master.main_release, 
  master.year, 
  master.genres, 
  master.styles
FROM 
  discogs.master
WHERE 
  master.id in (
  
  SELECT distinct
  release.master_id
FROM 
  discogs.release
WHERE 
  release.id in (Select release_id from   discogs.releases_artists where 
  releases_artists.artist_id = ? ) 
  and release.master_id is not null
);"
			);

			my $stmt_rels_master = $db->prepare(
				"SELECT 
  release.id as release_id, 
  release.title, 
  release.country, 
  release.released as released_date, 
  release.genres, 
  release.styles,
  release.master_id
FROM 
  discogs.release
WHERE 
  master_id = ? order by release.id ;"
			);

			my $stmt_rels = $db->prepare(
				"SELECT 
  release.id as release_id, 
  release.title, 
  release.country, 
  release.released as released_date, 
  release.genres, 
  release.styles,
  release.master_id
FROM 
  discogs.release
WHERE 
  release.id in (Select release_id from   discogs.releases_artists where 
  releases_artists.artist_id = ? )
  and release.master_id is null
  order by release.id ;"
			);

			my $stmt_rels_artist = $db->prepare(
				"SELECT 
  releases_artists.release_id, 
  releases_artists.artist_id, 
  releases_artists.artist_name, 
  releases_artists.anv, 
  releases_artists.join_relation, 
  releases_artists.\"position\"
FROM 
  discogs.releases_artists
WHERE 
  releases_artists.release_id = ? order by releases_artists.\"position\";"
			);

			my $stmt_tracks = $db->prepare(
				"SELECT 
 distinct track.release_id,
 track.position, 
  track.track_id, 
  track.title, 
  track.duration, 
  track.trackno 
FROM 
  discogs.track
WHERE 
  track.release_id = ? order by track.release_id;"
			);

			my $stmt_track_artists = $db->prepare(
				"Select 
  tracks_artists.track_id, 
  tracks_artists.\"position\", 
  tracks_artists.artist_name, 
  tracks_artists.artist_id, 
  tracks_artists.anv, 
  tracks_artists.join_relation
from
  discogs.tracks_artists
where 
  tracks_artists.track_id = ?;"
			);

			my $stmt_track_extraartists = $db->prepare(
				"Select 
  tracks_extraartists.track_id, 
  tracks_extraartists.artist_id, 
  tracks_extraartists.artist_name, 
  tracks_extraartists.anv, 
  tracks_extraartists.role
from
  discogs.tracks_extraartists
where 
  tracks_extraartists.track_id = ?;"
			);

			my $stmt_artist_details = $db->prepare(
				"Select 
				 artist.id, 
				 artist.name,
				 artist.realname,
				 artist.urls,
				 artist.namevariations,
				 artist.aliases,
				 artist.profile,
				 artist.members,
				 artist.groups
				 FROM discogs.artist
				 where
				 artist.id = ?;"
			);


			my $stmt_release_format = $db->prepare(
				"Select 
				 releases_formats.release_id, 
				 releases_formats.position,
				 releases_formats.format_name,
				 releases_formats.qty,
				 releases_formats.descriptions
				 FROM discogs.releases_formats
				 where
				 releases_formats.release_id = ?;"
			);


			#$stmt_id->execute();
			unless ( -d $artistid ) {
				unless ( mkdir( $artistid, 0755 ) ) {

#mkdir $id , 0755  die "Cannot create directory for $artistid and $artist_name.:$1 $!";
					$logger->info(
"Process $$: ID $artistid ,Cannot create directory for $artistid."
					);

					chdir("..");
					chdir("..");
					next;
				}
			}
			chdir $artistid;
			$logger->info(
				"Process $$: Release ID $id: executing release query");

			$stmt_rels->execute($artistid);
			my $rows = $stmt_rels->rows;
			$logger->info(
				"Process $$: Release ID $id: found $rows rows for release without master");

			while ( my $ref = $stmt_rels->fetchrow_hashref() ) {

		   #$logger->info("Process $$: Release ID $id: found data for release");
		   #my $id= $ref->{'release_id'};

				my $releaseObj = new Release(
					$ref->{'release_id'}, $ref->{'title'},
					$ref->{'country'},    $ref->{'released_date'},
					$ref->{'genres'},     $ref->{'styles'},
					$ref->{'master_id'}
				);

				my @release_artists = undef;
				$stmt_rels_artist->execute( $releaseObj->{'release_id'} );
				while ( my $refrelart = $stmt_rels_artist->fetchrow_hashref() )
				{
					my $releaseArtistsObj = new ReleaseArtists(
						$refrelart->{'release_id'},
						$refrelart->{'position'},
						$refrelart->{'artist_name'},
						$refrelart->{'artist_id'},
						$refrelart->{'anv'},
						$refrelart->{'join_relation'}

					);
					
					my @release_artist_details = undef;
					$stmt_artist_details->execute( $releaseArtistsObj->{'artist_id'} );
					
					while ( my $refartdetail = $stmt_artist_details->fetchrow_hashref() )
					{
					my $artistDetailsObj = new ArtistDetails(
						$refartdetail->{'id'},
						$refartdetail->{'name'},
						$refartdetail->{'realname'},
						$refartdetail->{'urls'},
						$refartdetail->{'namevariations'},
						$refartdetail->{'aliases'},
						$refartdetail->{'profile'},
						$refartdetail->{'members'},
						$refartdetail->{'groups'}

					);
					if($artistDetailsObj->{'artist_id'} eq 151641){
						$artistDetailsObj->{'namevariations'} =undef;
					}
						unshift( @release_artist_details, $artistDetailsObj );
					}
					$releaseArtistsObj->{'artistdetails'} =\@release_artist_details;
					
					unshift( @release_artists, $releaseArtistsObj );

				}
				$releaseObj->{'releaseartists'} = \@release_artists;

				my @tracks = undef;
				$stmt_tracks->execute( $releaseObj->{'release_id'} );
				while ( my $reftrack = $stmt_tracks->fetchrow_hashref() ) {
					my $trackObj = new Track(
						$reftrack->{'release_id'}, $reftrack->{'position'},'',
						$reftrack->{'title'},      $reftrack->{'duration'},
						$reftrack->{'trackno'}
					);

					my @trackArtist = undef;
					$stmt_track_artists->execute( $reftrack->{'track_id'} );
					while ( my $reftrackartist =
						$stmt_track_artists->fetchrow_hashref() )
					{
						my $trackartistObj = new TrackArtists(
							'',#$reftrackartist->{'track_id'},
							$reftrackartist->{'position'},
							$reftrackartist->{'artist_name'},
							$reftrackartist->{'artist_id'},
							$reftrackartist->{'anv'},
							$reftrackartist->{'join_relation'}
						);
						unshift( @trackArtist, $trackartistObj );
					}
					$trackObj->{'artists'} = \@trackArtist;
					
					my @trackextraArtist = undef;
					$stmt_track_extraartists->execute( $reftrack->{'track_id'} );
					while ( my $reftrackartist =
						$stmt_track_extraartists->fetchrow_hashref() )
					{
						my $trackextraartistObj = new TrackExtraArtists(
							'',#$reftrackartist->{'track_id'},
							$reftrackartist->{'artist_id'},
							$reftrackartist->{'artist_name'},
							$reftrackartist->{'anv'},
							$reftrackartist->{'role'}
						);
						unshift( @trackextraArtist, $trackextraartistObj );
					}
					$trackObj->{'extraartists'} = \@trackextraArtist;
					
					unshift( @tracks, $trackObj );
				}
				$releaseObj->{'tracks'} = \@tracks;
				
				my @release_format = undef;
				$stmt_release_format->execute($releaseObj->{'release_id'});
				while ( my $refformat = $stmt_release_format->fetchrow_hashref() ) {
					my $formatObj = new Format(
					$refformat->{'release_id'},
					$refformat->{'position'},
					$refformat->{'format_name'},
					$refformat->{'qty'},
					$refformat->{'descriptions'},
					);
					
					unshift( @release_format, $formatObj );
				}
				$releaseObj->{'formats'} = \@release_format;
				
				my $JSON = JSON->new->utf8(0);
				$JSON->convert_blessed(1);
				my $json = $JSON->encode($releaseObj);

			#$logger->info("Process $$: Release ID $id: release object $json ");

				my $filename =
				  'release_' . $releaseObj->{'release_id'} . '.json';
				  
				  my $rewrite = 1;
				  # from file content
					my $olddata = undef;
  					{
  						local $/ = undef;
  						$successoldfile = open( my $fh, '<:utf8', $filename );
  						if($successoldfile){
  							my $json_text   = <$fh>;
  							#$olddata = decode_json( $json_text );	
  							$logger->info("Process $$: found old json file with data");
  							if($json_text eq $json){
  								$rewrite = 0;
  								$logger->info("Process $$: No change in data, Skipping the update to file $filename.");	
  							}else{
  								$rewrite = 1;
  								$logger->info("different data old data: $json_text \n new data: $json");
  								$logger->info("Process $$: Data changed, updating the file $filename.");
  							} 
  							
  							
  						}else{
  							$rewrite = 1;
  							$logger->info("Process $$: cannot find old json file $filename, writing new file");
  						}
  					}
  					
  					if($rewrite){
						my $success0 = open ARTIST1, ">:utf8", $filename;    # or die $!;
						if ( !$success0 ) {
							$logger->info("Process $$: cannot create file");
		
							#print "Failed to create artist file for $artist_name\n $!";
						}
						else {
		
							#$real_name=~s/[\000-\037]/ /g;#remove ctrl char
							$logger->info(
		"Process $$: writing to file for release id $releaseObj->{'release_id'}"
							);
		
							print ARTIST1 $json;
		
							#print ARTIST "$real_name";
							
						}
						close ARTIST1;
  					}
			}

			$stmt_master->execute($artistid);
			$rows = $stmt_master->rows;
			$logger->info(
				"Process $$: Artist ID $artistid: found $rows rows for masters"
			);
			while ( my $mstrref = $stmt_master->fetchrow_hashref() ) {
				my $masterObj = new Master(
					$mstrref->{'id'},
					$mstrref->{'title'},
					$mstrref->{'main_release'},
					$mstrref->{'year'},
					$mstrref->{'genres'},
					$mstrref->{'styles'}
				);
				$stmt_rels_master->execute($masterObj->{'id'});
			my $rows = $stmt_rels_master->rows;
			$logger->info(
				"Process $$: Master ID $masterObj->{'id'}: found $rows rows for release with master");
			my @releaseObjlist = undef; 
			while ( my $ref = $stmt_rels_master->fetchrow_hashref() ) {

		   #$logger->info("Process $$: Release ID $id: found data for release");
		   #my $id= $ref->{'release_id'};

				my $releaseObj = new Release(
					$ref->{'release_id'}, $ref->{'title'},
					$ref->{'country'},    $ref->{'released_date'},
					$ref->{'genres'},     $ref->{'styles'},
					$ref->{'master_id'}
				);

				my @release_artists = undef;
				$stmt_rels_artist->execute( $releaseObj->{'release_id'} );
				while ( my $refrelart = $stmt_rels_artist->fetchrow_hashref() )
				{
					my $releaseArtistsObj = new ReleaseArtists(
						$refrelart->{'release_id'},
						$refrelart->{'position'},
						$refrelart->{'artist_name'},
						$refrelart->{'artist_id'},
						$refrelart->{'anv'},
						$refrelart->{'join_relation'}

					);
					
					my @release_artist_details = undef;
					$stmt_artist_details->execute( $releaseArtistsObj->{'artist_id'} );

					while ( my $refartdetail = $stmt_artist_details->fetchrow_hashref() )
					{
					my $artistDetailsObj = new ArtistDetails(
						$refartdetail->{'id'},
						$refartdetail->{'name'},
						$refartdetail->{'realname'},
						$refartdetail->{'urls'},
						$refartdetail->{'namevariations'},
						$refartdetail->{'aliases'},
						$refartdetail->{'profile'},
						$refartdetail->{'members'},
						$refartdetail->{'groups'}

					);
					if($artistDetailsObj->{'artist_id'} eq 151641){
						$artistDetailsObj->{'namevariations'} =undef;
					}
						unshift( @release_artist_details, $artistDetailsObj );
					}
					$releaseArtistsObj->{'artistdetails'} =\@release_artist_details;
					unshift( @release_artists, $releaseArtistsObj );

				}
				$releaseObj->{'releaseartists'} = \@release_artists;

				my @tracks = undef;
				$stmt_tracks->execute( $releaseObj->{'release_id'} );
				while ( my $reftrack = $stmt_tracks->fetchrow_hashref() ) {
					my $trackObj = new Track(
						$reftrack->{'release_id'}, $reftrack->{'position'},'', 
						$reftrack->{'title'},      $reftrack->{'duration'},
						$reftrack->{'trackno'}
					);

					my @trackArtist = undef;
					$stmt_track_artists->execute( $reftrack->{'track_id'} );
					while ( my $reftrackartist =
						$stmt_track_artists->fetchrow_hashref() )
					{
						my $trackartistObj = new TrackArtists(
							'',#$reftrackartist->{'track_id'},
							$reftrackartist->{'position'},
							$reftrackartist->{'artist_name'},
							$reftrackartist->{'artist_id'},
							$reftrackartist->{'anv'},
							$reftrackartist->{'join_relation'}
						);
						
					my @track_artist_details = undef;
					$stmt_artist_details->execute( $trackartistObj->{'artist_id'} );

					while ( my $refartdetail = $stmt_artist_details->fetchrow_hashref() )
					{
					my $artistDetailsObj = new ArtistDetails(
						$refartdetail->{'id'},
						$refartdetail->{'name'},
						$refartdetail->{'realname'},
						$refartdetail->{'urls'},
						$refartdetail->{'namevariations'},
						$refartdetail->{'aliases'},
						$refartdetail->{'profile'},
						$refartdetail->{'members'},
						$refartdetail->{'groups'}

					);
					if($artistDetailsObj->{'artist_id'} eq 151641){
						$artistDetailsObj->{'namevariations'} =undef;
					}
						unshift( @track_artist_details, $artistDetailsObj );
					}
					$trackartistObj->{'artistdetails'} =\@track_artist_details;
						
						unshift( @trackArtist, $trackartistObj );
					}
					$trackObj->{'artists'} = \@trackArtist;
					
					my @trackextraArtist = undef;
					$stmt_track_extraartists->execute( $trackObj->{'track_id'} );
					while ( my $reftrackartist =
						$stmt_track_extraartists->fetchrow_hashref() )
					{
						my $trackextraartistObj = new TrackExtraArtists(
							'',#$reftrackartist->{'track_id'},
							$reftrackartist->{'artist_id'},
							$reftrackartist->{'artist_name'},
							$reftrackartist->{'anv'},
							$reftrackartist->{'role'}
						);
						
					my @track_artist_details = undef;
					$stmt_artist_details->execute( $trackextraartistObj->{'artist_id'} );

					while ( my $refartdetail = $stmt_artist_details->fetchrow_hashref() )
					{
					my $artistDetailsObj = new ArtistDetails(
						$refartdetail->{'id'},
						$refartdetail->{'name'},
						$refartdetail->{'realname'},
						$refartdetail->{'urls'},
						$refartdetail->{'namevariations'},
						$refartdetail->{'aliases'},
						$refartdetail->{'profile'},
						$refartdetail->{'members'},
						$refartdetail->{'groups'}

					);
					
					if($artistDetailsObj->{'artist_id'} eq 151641){
						$artistDetailsObj->{'namevariations'} =undef;
					}
						unshift( @track_artist_details, $artistDetailsObj );
					}
					$trackextraartistObj->{'artistdetails'} =\@track_artist_details;
						
						unshift( @trackextraArtist, $trackextraartistObj );
					}
					$trackObj->{'extraartists'} = \@trackextraArtist;
					
					
					unshift( @tracks, $trackObj );
				}
				$releaseObj->{'tracks'} = \@tracks;
				
				my @release_format = undef;
				$stmt_release_format->execute($releaseObj->{'release_id'});
				while ( my $refformat = $stmt_release_format->fetchrow_hashref() ) {
					my $formatObj = new Format(
					$refformat->{'release_id'},
					$refformat->{'position'},
					$refformat->{'format_name'},
					$refformat->{'qty'},
					$refformat->{'descriptions'},
					);
					
					unshift( @release_format, $formatObj );
				}
				$releaseObj->{'formats'} = \@release_format;
				
				
				unshift(@releaseObjlist,$releaseObj);
			}
			$masterObj->{'releaselist'} = \@releaseObjlist; 
			
			my $JSON = JSON->new->utf8(0);
				$JSON->convert_blessed(1);
				my $json = $JSON->encode($masterObj);

			#$logger->info("Process $$: Release ID $id: release object $json ");

				my $filename =
				  'master_' . $masterObj->{'id'} . '.json';
				 
				  my $rewrite = 1;
				  # from file content
  					{
  						local $/ = undef;
  						$successoldfile = open( my $fh, '<:utf8', $filename );
  						if($successoldfile){
  							my $json_text   = <$fh>;
  							#$olddata = decode_json( $json_text );	
  							$logger->info("Process $$: found old json file with data");
  							if($json_text eq $json){
  								$rewrite = 0;
  								$logger->info("Process $$: No change in data, Skipping the update to file $filename.");
  							}else{
  								$rewrite = 1;
  								$logger->info("Process $$: Data changed, updating the file $filename.");
  							} 
  							
  							
  						}else{
  							$rewrite = 1;
  							$logger->info("Process $$: cannot find old json file $filename, writing new file");
  						}
  					}
  					if($rewrite){
						my $success0 = open MASTER, ">:utf8", $filename;    # or die $!;
						if ( !$success0 ) {
							$logger->info("Process $$: cannot create file");
		
							#print "Failed to create artist file for $artist_name\n $!";
						}
						else {
		
							#$real_name=~s/[\000-\037]/ /g;#remove ctrl char
							$logger->info(
		"Process $$: writing to file for release id $releaseObj->{'release_id'}"
							);
		
							print MASTER $json;
		
							#print ARTIST "$real_name";
							
						}
						close MASTER;
  				}
			
			}
			#$logger->info("Completing Process $$");

			#exit 0;

			chdir("..");
			chdir("..");
			chdir("..");
		}
	}
	$db->disconnect();
}

sub sendToLogfile {
##Logging settings#####
  #my $log_conf = q(
  #   log4perl.rootLogger              = DEBUG, LOG1, SCREEN
  #   log4perl.appender.SCREEN         = Log::Log4perl::Appender::Screen
  #   log4perl.appender.SCREEN.stderr  = 0
  #   log4perl.appender.SCREEN.layout  = Log::Log4perl::Layout::PatternLayout
  #   log4perl.appender.SCREEN.layout.ConversionPattern = %m %n
  #   log4perl.appender.LOG1           = Log::Log4perl::Appender::File
  #   log4perl.appender.LOG1.filename  = /res/users/gjamuar/DataExtracterlog.log
  #   log4perl.appender.LOG1.mode      = append
  #   log4perl.appender.LOG1.layout    = Log::Log4perl::Layout::PatternLayout
  #   log4perl.appender.LOG1.layout.ConversionPattern = %d %p %m %n
  #);
	my $log_conf = LOG_SETTING;
	Log::Log4perl::init( \$log_conf );

	my $logger = Log::Log4perl->get_logger();
##End Logging settings#####

	my (@array) = @_;

	$logger->error("Unexpected error: @array");

}
