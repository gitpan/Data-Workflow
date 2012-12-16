package Data::Workflow::Util;
#################################################################################
#
# Author:  Nat Goodman
# Created: 12-12-16
# $Id: 
#
# Generic utility functions like min and max, which are mostly provided via 
# CPAN modules such as List::Util, and some utility functions of our own
#
#################################################################################
use strict;
use Carp;
use List::Util @List::Util::EXPORT_OK;
use List::MoreUtils @List::MoreUtils::EXPORT_OK;
use Scalar::Util @Scalar::Util::EXPORT_OK;
use Exporter();
our @ISA=qw(Exporter);
# NG 11-01-05: added parse_hash, parse_hash
our @EXPORT_OK=(@List::Util::EXPORT_OK,@List::MoreUtils::EXPORT_OK,@Scalar::Util::EXPORT_OK,
		qw(flatten group groupmap parse_list set_defaults
		   parse_hash parse_hashl
		   choose_basepath choose_file dezip_file out_dir 
		   open_version open_file open_dir openmode 
		   fileparse fileparse2 fileparse3 dir file base suffix final_suffix
		   catname splitname
		   clean_id clean_path));
################################################################################
# from GDxBase::Utils::Util

sub flatten {map {'ARRAY' eq ref $_? @$_: $_} @_;}

# group a list by categories returned by sub.
# has to be declared before use, because of prototype
sub group (&@) {
  my($sub,@list)=@_;
  my %groups;
  for (@list) {
    my $group=&$sub($_);
    my $members=$groups{$group} || ($groups{$group}=[]);
    push(@$members,$_);
  }
  wantarray? %groups: \%groups;
}
# like group, but processes elements that are put on list. 
# sub should return 2 element list: 1st defines group, 2nd maps the value
# has to be declared before use, because of prototype
sub groupmap (&@) {
  my($sub,@list)=@_;
  my %groups;
  for (@list) {
    my($group,$value)=&$sub($_);
    my $members=$groups{$group} || ($groups{$group}=[]);
    push(@$members,$value);
  }
  wantarray? %groups: \%groups;
}

# parse list of filenames or other sources.
sub parse_list {
  my $list;
  if (@_==1 && 'ARRAY' eq ref $_[0]) {     # called with ARRAY ref
    $list=$_[0];
  } elsif (@_==1 && ref $_[0]) {           # called with some other ref, eg GLOB
    $list=[$_[0]];
  }
  else {		           # called with one or more strings
    $list=[];
    @$list=map {split(/\s+/,$_)} @_; # split into words
  }
  wantarray? @$list: $list;
}
################################################################################
# from Data::Pipeline::Util

# NG 11-01-05: adapted from my never released config file parsing code circa 09-Q1
# parse text hash into perl hash
# input is pairs of words optionally connected by => or = and separated by whitespace or comma
# eg, key1=>value1, key2 value2 key3 = value3
sub parse_hash {
  my($string)=@_;
  $string=~s/\s*(=>|=)\s*/ /g;
  # parse_list is overkill. just split on whitespace
  # my %hash=parse_list($string);
  my %hash=split(/\s+/,$string);
  wantarray? %hash: \%hash;
}
# parse line-oriented text hash into perl hash
# input is lines of the form key => value or key = value or key value
sub parse_hashl {
  my @lines=split("\n",$_[0]);
  map {s/^\s+|\s+$//g} @lines;  # trim whitespace
  my %hash=map {/^(.*?)\s*(?:=>|=|\s|$)\s*(.*)$/} @lines;
  wantarray? %hash: \%hash;
}

# adapted from Data::Pipeline & Data::ResourcePool
# set default values in hash.
sub set_defaults (\%\%) {
  my($actuals,$defaults)=@_;
  while(my($key,$default)=each %$defaults) {
    next if defined $actuals->{$key};
    $actuals->{$key}=$defaults->{$key};
  }
  wantarray? %$actuals: $actuals;
}

################################################################################
# from Data::ResourcePool::Util
sub open_version {
  my($mode,$version,$create_paths)=@_==1? ('r',$_[0],undef): @_;
  # NG 10-12-29: if version is directory, open as such
  # open_file($mode,$version->full_id,$create_paths);
  my $path=$version->full_id;
  !-d $path? open_file($mode,$path,$create_paths): open_dir($mode,$path,$create_paths);
}

sub open_file {
  my($mode,$file,$create_paths)=@_==1? ('r',$_[0],undef): @_;
  # NG 10-12-29: use new openmode function
  # $mode='<s' if $mode && $mode=~/^rs|^ru/;
  # $mode='<'  if !$mode || $mode=~/^r/;
  # $mode='>s' if $mode=~/^ws|^wu/;	# must come before next line
  # $mode='>'  if $mode=~/^w/;
  # $mode='>>' if $mode=~/^a/;
  $mode=openmode($mode);
  my $fh;
  if ($mode eq '<') {		# for read mode, 
    $file=choose_file($file);
    my $open_arg=dezip_file($file);
    open($fh,$open_arg) || confess "Cannot open file $file for read: $!";
  } elsif ($mode eq '<s') {
    $file=choose_file($file);
    my $dezip=dezip_file($file);
    my $open_arg;
    if ($dezip=~s/^<//) {
      $open_arg="sort -u $dezip |";
    } else {
      $open_arg="$dezip sort -u |"; # $dezip already has trailing '|'
    }
    open($fh,$open_arg) || confess "Cannot open file $file for read: $!";
  } else {
    if ($mode eq '>') {
      out_dir($file,$create_paths);
      my $open_arg="$mode $file";
      open($fh,$open_arg) || confess "Cannot create file $file: $!";
    } elsif ($mode eq '>s') {
      out_dir($file,$create_paths);
      my $open_arg="| sort -u > $file";
      open($fh,$open_arg) || confess "Cannot open file $file for writing with sort('$open_arg'): $!";
    } elsif ($mode eq '>>') {
      my $open_arg="$mode $file";
      open($fh,$open_arg) || confess "Cannot open file $file for append: $!";
    } else {
      confess "Unrecognized open mode $mode";
    }}
  $fh;
}
# NG 10-12-29: added open_dir to support interactions directory for InnateDB
#              what Perl's opendir really does is provide access to list of filenames in
#              directory. emulate by reading filenames into string, and opening regular
#              filehandle to string
sub open_dir {
  my($mode,$dir,$create_paths)=@_==1? ('r',$_[0],undef): @_;
  $mode=openmode($mode);
  confess "Attempting to open directory $dir in write mode (mode=$mode)" unless $mode=~/^</;
  # create directory if necessary and possible
  out_dir($dir,$create_paths,'path_is_dir');
  opendir(DIR,$dir) || confess "Cannot open directory $dir for read: $!";
  my @files=grep !/^\.{1,2}$/,readdir DIR;
  @files=sort @files if $mode eq '<s';
  my $files=join("\n",@files);
  my $fh;
  open($fh,'<',\$files) || confess "Bad news: open filehandle to string failed: $!";
  $fh;
}

# NG 08-09-23: not used. see dezip_file
# uncompress file in-place
# sub unzip_file {
#   my($file)=@_;
#   my $infile=choose_file($file);
#   my($outfile)=$file=~/^(.*)\..*$/;
#   unless ($infile eq $outfile) {
#     my $cmd;
#     $cmd="gunzip -c $infile > $outfile" if $infile=~/\.gz$/;
#     $cmd="uncompress -c $infile > $outfile" if $infile=~/\.Z$/;
#     $cmd="bunzip2 -c $infile > $outfile" if $infile=~/\.bz2$/;
#     $cmd="unzip -p $infile > $outfile" if $infile=~/\.zip$/;
#     $cmd="tar xfzO $file |" if $infile=~/\.tgz/; # NG 08-07-09
#     system($cmd)==0 || confess "$cmd failed: $?";
#   }
#   $outfile;
# }

# NG 10-12-29: refactored code that standardizes open modes
sub openmode {
  my($mode)=@_;
  $mode='<s' if $mode && $mode=~/^rs|^ru/;
  $mode='<'  if !$mode || $mode=~/^r/;
  $mode='>s' if $mode=~/^ws|^wu/;	# must come before next line
  $mode='>'  if $mode=~/^w/;
  $mode='>>' if $mode=~/^a/;
  $mode;
}

# NG 08-07-09: added tgz
# NG 08-09-24: return suffix, too. needed by copy
our @suffixes=qw(gz Z bz2 zip tgz);
sub choose_file {
  my($file,$autoconvert)=@_;
  defined $autoconvert or $autoconvert=1;
  my $stat=stat $file;
  my $suffix;
  if ($autoconvert) {
    # NG 06-06-8: regexp completely wrong! scary this bug wasn't found sooner...
    # my($suffix)=grep {$file=~/^(.*)\.$_$/; $1} @suffixes;   # what's the suffix, if any?
    ($suffix)=grep {$file=~/^.*\.$_$/? $_: undef} @suffixes; # what's the suffix, if any?
    if ($suffix) {		                   # desired file has a suffix
      my($file_base)=$file=~/^(.*)\.$suffix$/;
      my $stat_base=stat $file_base;
      if (is_file($stat) && is_file($stat_base)) { # both files exist -- use newest one
	($stat->mtime > $stat_base->mtime) or ($file,$suffix,$stat)=($file_base,undef,$stat_base);
      } elsif (is_file($stat_base)) {	           # base is only one that exists
	($file,$suffix,$stat)=($file_base,undef,$stat_base);
      }	    # else original is only one that exists or neither exists.
      # use original in either case. if doesn't exist, caller will handle
    } elsif (!is_file($stat)) {	                   # desired file has no suffix and doesn't exist
      # try with each suffix and use first one that exists
      # NG 11-04-18: for some reason, $suffix is being localized here. so use different variable
      # for $suffix (@suffixes) {
      for my $sfx (@suffixes) {
	my $file_sfx="$file.$sfx";
	my $stat_sfx=stat $file_sfx;
	($file,$suffix,$stat)=($file_sfx,$sfx,$stat_sfx),last if is_file($stat_sfx);
      } # at end of loop, ($file,$suffix,$stat) is either first one that exists, or original
    }   # else, desired file has no suffix and exists, so use it
  }
  wantarray? ($file,$suffix,$stat): $file;
}
# return path with known suffixes removed
sub choose_basepath {
  my($file,$autoconvert)=@_;
  defined $autoconvert or $autoconvert=1;
  return $file unless $autoconvert;
  my $basepath;
  for my $suffix (@suffixes) {
    ($basepath)=$file=~/^(.*)\.$suffix$/;
    last if $basepath;
  }
  $basepath || $file;
}

sub is_file {		# path exists & is not directory
  my($stat)=@_;
  $stat && !S_ISDIR($stat->mode);
}

# Based on Lincoln Stein's setup_argv from BioPerl
sub dezip_file {
  my($file)=@_;
  return undef unless $file;
  return "gunzip -c $file |" if $file=~/\.gz$/;
  # NG 08-10-01: zcat seems to be more widely available than uncompress
  # return "uncompress -c $file |" if $file=~/\.Z$/;
  return "zcat $file |" if $file=~/\.Z$/;
  return "bunzip2 -c $file |" if $file=~/\.bz2$/;
  return "unzip -p $file |" if $file=~/\.zip$/;
  return "tar xfzO $file |" if $file=~/\.tgz/; # NG 08-07-09
  return "< $file";
}
# TODO: improve error reporting
# check output paths. create if necessary and possible
sub out_dir {
  my($path,$create_paths,$path_is_dir)=@_;
  my($out_dir,$out_file)=$path_is_dir? ($path,undef): fileparse($path);
  return $out_dir if -d $out_dir && -w _;
  # TODO: log errors instead of confessing
  my $errstr="Cannot create output directory '$out_dir'";
  confess "$errstr: directory exists but is not writable" if -e _;
  # try to create directory
  confess "$errstr: directory does not exist and could not be created: create_paths parameter set to FALSE" unless $create_paths;
  eval {mkpath($out_dir)};
  if ($@) {
    confess "$errstr: directory does not exist and could not be created: $@";
  }
  # else success
  $out_dir;
}

# Patterns in fileparse methods adapted from File::Basename 
sub fileparse {
  my($path)=@_;
  my($dir,$file)=($path =~ m#^(.*/)*(.*?)$#s);
  ($dir,$file);
}
*fileparse2=\&fileparse;
sub fileparse3 {
  my($path)=@_;
  my($dir,$base,$sfx)=($path =~ m#^(.*/)*(.*?)(\..*)*$#s);
  ($dir,$base,$sfx);
}
sub dir {
  my($path)=@_;
  my($dir)=fileparse($path);
  $dir;
}
sub file {
  my($path)=@_;
  my($dir,$file)=fileparse($path);
  $file;
}
sub base {
  my($path)=@_;
  my($dir,$base,$suffix)=fileparse3($path);
  $base;
}
sub suffix {
  my($path)=@_;
  my($dir,$base,$suffix)=fileparse3($path);
  $suffix;
}
sub final_suffix {
  my($path)=@_;
  my $suffix=suffix($path);
  my($final_suffix)=($suffix=~m#^.*(\..*?)*$#s);
  $final_suffix;
}

sub clean_path {
  my($path)=@_;
  $path=~s/\s*\/\s*/\//g;	# strip spaces around /'s
  $path=~s/\/+/\//g;		# fix repeated /'s 
  $path=~s/^\s+|\s+$//g;	# strip leading or trailing spaces
  $path=~s/\/$//g;		# strip trailing /
  $path=~s/^(\w+):\//$1:\/\//;	# put back double slash at beginning of URIs
  $path;
}

# from Data::ResourcePool. same as clean_path, except also strips leading /
# TODO; decide whether this function can be replaced by clean_path
# clean up leading and trailing whitespace or /'s, and doubled /'s
sub clean_id {
  my($id)=@_;
  $id=~s/\s*\/\s*/\//g;		# strip spaces around /'s
  $id=~s/\/+/\//g;		# fix repeated /'s 
  $id=~s/^\s+|\s+$//g;		# strip leading or trailing spaces
  $id=~s/^\/|\/$//g;		# string leading or trailing /
  $id=~s/^(\w+):\//$1:\/\//;	# put back double slash at beginning of URIs
  $id;
}

# next two are from Data::Pipeline
# concatenate name components, ignoring any blanks
sub catname {join('/',grep {$_} @_);}
# split name into components. 
sub splitname {
  my @list=split(/\/+/,$_[0]);
  wantarray? @list: \@list;
}
1;

