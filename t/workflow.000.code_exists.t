#!perl
use strict;
use Test::More;
# make sure all the necesary modules exist and compile
BEGIN {
  use_ok( 'Data::Workflow' );
  use_ok('Data::Workflow::BFSort');
  use_ok('Data::Workflow::GraphAlgorithms');
  use_ok('Data::Workflow::Log');
  use_ok('Data::Workflow::Log::Object');
  use_ok('Data::Workflow::Namespace');
  use_ok('Data::Workflow::Namespace::database');
  use_ok('Data::Workflow::Namespace::null');
  use_ok('Data::Workflow::Namespace::path');
  use_ok('Data::Workflow::Namespace::uri');
  use_ok('Data::Workflow::Resource');
  use_ok('Data::Workflow::ResourcePool');
  use_ok('Data::Workflow::Step');
  use_ok('Data::Workflow::Step::cat');
  use_ok('Data::Workflow::Step::copy');
  use_ok('Data::Workflow::Step::copy::Operation');
  use_ok('Data::Workflow::Step::database');
  use_ok('Data::Workflow::Step::database::append');
  use_ok('Data::Workflow::Step::database::create');
  use_ok('Data::Workflow::Step::database::create_schema');
  use_ok('Data::Workflow::Step::grep');
  use_ok('Data::Workflow::Step::perl');
  use_ok('Data::Workflow::Step::touch');
  use_ok('Data::Workflow::Step::untar');
  use_ok('Data::Workflow::Util');
  use_ok('Data::Workflow::Version');
  use_ok('Data::Workflow::VersionMap');
}
diag( "Testing Data::Workflow $Data::Workflow::VERSION, Perl $], $^X" );
done_testing();
