#!perl
use strict;
use Test::More;

SKIP: {
  # Ensure a recent version of Test::Pod
  my $min_tp = 1.22;
  eval "use Test::Pod $min_tp";
  skip "POD test: Test::Pod $min_tp required",1 if $@;
  my @pod_files=all_pod_files();
  map {pod_file_ok($_)} @pod_files;
}

SKIP: {
  # Ensure a version of Test::Pod::Content
  eval "use Test::Pod::Content";
  skip 'version number in POD: Test::Pod::Content required',1 if $@;
  use t::Build;
  my $builder=t::Build->current;
  my $module_pm=File::Spec->catdir('blib',$builder->dist_version_from);
  my $correct_version=$builder->dist_version;
  $correct_version=~s/_.*//;	# strip development sub-version number
  pod_section_like($module_pm,'VERSION',qr/Version $correct_version$/,'version number in POD');
}

done_testing();
