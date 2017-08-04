require 'base64'
require 'json'
require 'csv'
require 'tempfile'
require 'http'

module Embulk
  module Input

    class Lkqd < InputPlugin
      Plugin.register_input("lkqd", self)

      def self.transaction(config, &control)
        task = {
          "secret_key_id" => config.param("secret_key_id", :string),
          "secret_key" => config.param("secret_key", :string),
          "endpoint" => config.param("endpoint", :string, default: 'https://api.lkqd.com/reports'),
          "report_parameters" => config.param("report_parameters", :hash, default: {}),
        }
        task['authorization'] = Base64.urlsafe_encode64("#{task['secret_key_id']}:#{task['secret_key']}")

        response = request_lkqd({authorization: task['authorization'], endpoint: task['endpoint'], report_parameters: task['report_parameters']})
        tempfile = Tempfile.new('lkqd_')
        tempfile.write response.body
        task['tempfile_path'] = tempfile.path
        tempfile.close

        columns = []
        ::CSV.foreach(tempfile.path).first.each_with_index do |column_name, index|
          column_type = guess_column_type(task['report_parameters']['metrics'], column_name)
          columns << Column.new({index: index}.merge(column_type))
        end

        resume(task, columns, 1, &control)
      end

      def self.resume(task, columns, count, &control)
        task_reports = yield(task, columns, count)

        next_config_diff = {}
        return next_config_diff
      end

      def self.guess(config)
        return {}
      end

      def self.request_lkqd(config)
        ::HTTP.auth("Basic #{config[:authorization]}").post(config[:endpoint], json: config[:report_parameters])
      end

      def self.guess_column_type(metrics, column_name)
        column_name.gsub!(/^\W/, '')
        column_option = DEFAULT_COLUMNS[column_name]
        if column_option
          return {type: column_option['out_type'].to_sym, name: column_name, format: column_option['format']}
        else
          return {type: :string, name: column_name}
        end
      end

      def init
        @task = task
      end

      def run
        CSV.foreach(@task['tempfile_path'], {headers: true}).each do |row|
          page_builder.add(row)
        end
        page_builder.finish

        task_report = {}
        return task_report
      end

      DEFAULT_COLUMNS = {
        # dimensions' columns
        'Time'=> { 'out_type' => 'timestamp', 'format' => "%Y-%m-%dT%H", 'timezone' => 'UTC' },
        'Account'=> { 'out_type' => 'string' },
        'Supply Source ID'=> { 'out_type' => 'string' },
        'Supply Source'=> { 'out_type' => 'string' },
        'Supply Partner'=> { 'out_type' => 'string' },
        'Environment'=> { 'out_type' => 'string' },
        'Domain'=> { 'out_type' => 'string' },
        'App Name'=> { 'out_type' => 'string' },
        'Bundle ID'=> { 'out_type' => 'string' },
        'Supply Tag Type'=> { 'out_type' => 'string' },
        'Demand Partner'=> { 'out_type' => 'string' },
        'Demand Deal ID' => { 'out_type' => 'string' },
        'Demand Deal'=> { 'out_type' => 'string' },
        'Demand Tag'=> { 'out_type' => 'string' },
        'Format'=> { 'out_type' => 'string' },
        'Country'=> { 'out_type' => 'string' },
        'Device Type'=> { 'out_type' => 'string' },
        'OS'=> { 'out_type' => 'string' },
        'Width X Height'=> { 'out_type' => 'string' },
        # "Opportunity report" columns
        'Tag Loads' => { 'out_type' => 'long' },
        'Opportunities' => { 'out_type' => 'long' },
        'Format Loads' => { 'out_type' => 'long' },
        'Format Fill Rate' => { 'out_type' => 'double' },
        'Ineligible Ops: Demand' => { 'out_type' => 'long' },
        'Ineligible Ops: Restrictions' => { 'out_type' => 'long' },
        'Impressions' => { 'out_type' => 'long' },
        'Fill Rate' => { 'out_type' => 'double' },
        'Efficiency Rate' => { 'out_type' => 'double' },
        'CPM' => { 'out_type' => 'double' },
        'Revenue' => { 'out_type' => 'double' },
        'Cost' => { 'out_type' => 'double' },
        'Profit' => { 'out_type' => 'double' },
        'Profit Margin' => { 'out_type' => 'double' },
        'Clicks' => { 'out_type' => 'long' },
        'CTR' => { 'out_type' => 'double' },
        '25% Views' => { 'out_type' => 'long' },
        '50% Views' => { 'out_type' => 'long' },
        '75% Views' => { 'out_type' => 'long' },
        '100% Views' => { 'out_type' => 'long' },
        '25% View Rate' => { 'out_type' => 'double' },
        '50% View Rate' => { 'out_type' => 'double' },
        '75% View Rate' => { 'out_type' => 'double' },
        '100% View Rate' => { 'out_type' => 'double' },
        'Viewability Measured Rate' => { 'out_type' => 'double' },
        'Viewability Rate' => { 'out_type' => 'double' },
        # "Request report" columns
        'Tag Requests' => { 'out_type' => 'long' },
        'Ads' => { 'out_type' => 'long' },
        'VAST Ads' => { 'out_type' => 'long' },
        'VPAID Ads' => { 'out_type' => 'long' },
        'Wins' => { 'out_type' => 'long' },
        'Ad Rate' => { 'out_type' => 'double' },
        'VAST Ad Rate' => { 'out_type' => 'double' },
        'VPAID Ad Rate' => { 'out_type' => 'double' },
        'Win Rate' => { 'out_type' => 'double' },
        'VPAID Responses' => { 'out_type' => 'long' },
        'VPAID Attempts' => { 'out_type' => 'long' },
        'VPAID Successes' => { 'out_type' => 'long' },
        'VPAID Opt Outs' => { 'out_type' => 'long' },
        'VPAID Timeouts' => { 'out_type' => 'long' },
        'VPAID Errors' => { 'out_type' => 'long' },
        'VPAID Success Rate' => { 'out_type' => 'double' },
        'VPAID Opt Out Rate' => { 'out_type' => 'double' },
        'VPAID Timeout Rate' => { 'out_type' => 'double' },
        'VPAID Error Rate' => { 'out_type' => 'double' },
        'Tag Timeouts' => { 'out_type' => 'long' },
        'Tag Timeout Rate' => { 'out_type' => 'double' },
        'Tag Errors' => { 'out_type' => 'long' },
        'Tag Error Rate' => { 'out_type' => 'long' },
        'Playback Errors' => { 'out_type' => 'long' },
        'Playback Error Rate' => { 'out_type' => 'double' },
        # custom data columns
        'Custom 1'=> { 'out_type' => 'string' },
        'Custom 2'=> { 'out_type' => 'string' },
        'Custom 3'=> { 'out_type' => 'string' },
        'Custom 4'=> { 'out_type' => 'string' },
        'Custom 5'=> { 'out_type' => 'string' },
        'Custom 6'=> { 'out_type' => 'string' },
        'Custom 7'=> { 'out_type' => 'string' },
        'Custom 8'=> { 'out_type' => 'string' },
        'Custom 9'=> { 'out_type' => 'string' },
        'Custom 10'=> { 'out_type' => 'string' },
      }.freeze
    end

  end
end

