require "rspec/its"

$spec_dir = File.expand_path(File.dirname(__FILE__))
require $spec_dir+"/helper"
$hosts = $spec_dir+"/hosts"

RSpec.configure do |config|
  config.filter_run :focus
  config.run_all_when_everything_filtered = true
end

Helper.show=false

describe Helper do

  %w[ h V D n N P q g G T t v W X j ].each do |a|
    context "dir=001 arg=-#{a}" do
      subject { Helper.new("001","-"+a).run }
      it { should be_success }
    end
  end

  %w[ N q g G t v X j ].each do |a|
    context "dir=002 arg=-#{a}" do
      subject { Helper.new("002","-"+a).clean.run }
      it { should be_success }
      its(:n_files) { should eq 9 }
    end
  end

  if File.exist?($hosts)
    context "dir=002 --hostfile" do
      subject { Helper.new("002","-F ../hosts").clean.run }
      it { should be_success }
      its(:n_files) { should eq 9 }
    end
  end

  context "dir=003 w task argument" do
    subject { Helper.new("003","hello[foo,bar]").run }
    it { should be_success }
    its(:result) { should eq "first x=foo,y=bar\nhello x=foo,y=bar\n" }
  end

  context "dir=004 -j4 elapsed time" do
    subject { Helper.new("004","-j4").run }
    it { should be_success }
    its(:elapsed_time) { should be_within(1).of(2) } # 1..3 sec
  end

  if File.exist?($hosts)
    context "dir=005 --hostfile" do
      subject { Helper.new("005","-q -F ../hosts").run }
      it { should be_success }
      #its("output_lines.sort") { should eq Helper.read_hosts($hosts,true).sort }
    end
  end

  context "dir=006 PASS_ENV" do
    subject { Helper.new("006","-q -F ../hosts ENV1=pass_successfully").run }
    it { should be_success }
    its(:result) { should eq "pass_successfully\n" }
  end

  ## context "dir=007 invoke-in-task", :focus=>true do
  #context "dir=007 invoke-in-task" do
  #  subject { Helper.new("007","-j10").run }
  #  it { should be_success }
  #  its(:elapsed_time) { should be_within(1).of(2) } # 1..3 sec
  #end

  context "dir=008 --show-conf & PASS_ENV" do
    subject { Helper.new("008","--show-conf ENV1=hoge").run }
    it { should be_success }
    its(:result) { should match(/  ENV1: hoge/) }
  end

  context "dir=009 PROFILE w GNU_TIME" do
    subject { Helper.new("009").clean.run }
    it { should be_success }
    its(:n_files) { should eq 3 }
  end

  context "dir=010 invoke in Rakefile" do
    subject { Helper.new("010").run }
    it { should be_success }
  end

  context "dir=011 FAILD_TARGET=delete" do
    subject { Helper.new("011","FAILED_TARGET=delete").clean.run }
    it { should_not be_success }
    its(:n_files) { should eq 2 }
  end

  context "dir=011 FAILD_TARGET=rename" do
    subject { Helper.new("011","FAILED_TARGET=rename").clean.run }
    it { should_not be_success }
    its(:n_files) { should eq 3 }
  end

end
