require 'base64'
require 'json'
require 'csv'
require 'fileutils'
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
          "copy_temp_to" => config.param("copy_temp_to", :string, default: nil),
          "measurable_impressions" => config.param("measurable_impressions", :bool, default: false),
          "viewable_impressions" => config.param("viewable_impressions", :bool, default: false),
          "report_parameters" => config.param("report_parameters", :hash, default: {}),
        }
        task['authorization'] = Base64.urlsafe_encode64("#{task['secret_key_id']}:#{task['secret_key']}")

        response = request_lkqd({authorization: task['authorization'], endpoint: task['endpoint'], report_parameters: task['report_parameters']})
        tempfile = Tempfile.new('emublk-input-lkqd_')
        while chunk = response.body.readpartial
          tempfile.write chunk
        end
        tempfile.close
        task['tempfile_path'] = tempfile.path

        FileUtils.cp(task['tempfile_path'], task['copy_temp_to']) if task['copy_temp_to']

        columns = []
        ::CSV.foreach(tempfile.path).first.each_with_index do |column_name, index|
          column_type = guess_column_type(task['report_parameters']['metrics'], column_name)
          column = Column.new({index: index}.merge(column_type))
          if column.name == 'Viewability Measured Rate'
            task['viewability_measured_rate_index'] = index
          elsif column.name == 'Viewability Rate'
            task['viewability_rate_index'] = index
          elsif column.name == 'Impressions'
            task['impressions_index'] = index
          end
          columns << column
        end

        if measurable_impressions?(task)
          columns << Column.new({index: columns.size, type: :double, name: 'Measurable Impressions'})
        end

        if viewable_impressions?(task)
          columns << Column.new({index: columns.size, type: :double, name: 'Viewable Impressions'})
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
          return {type: column_option['type'].to_sym, name: column_name, format: column_option['format']}
        else
          return {type: :string, name: column_name}
        end
      end

      def self.try_convert(row, options={})
        return row.map do |field|
          name, value = field
          column_name = name.gsub(/^\W/, '')
          column_option = DEFAULT_COLUMNS[column_name]
          if column_option.nil?
            next value
          elsif column_option['type'] == 'timestamp'
            next Time.strptime(value + " " + options[:timezone], column_option['format'] + " %Z").to_i
          elsif column_option['type'] == 'long'
            next value.gsub(',','').to_i
          elsif column_option['type'] == 'double' && value.match(/%$/)
            next value.gsub(',','').to_f / 100.0
          elsif column_option['type'] == 'double'
            next value.gsub(',','').to_f
          else
            next value
          end
        end
      end

      def self.measurable_impressions?(task)
        task['measurable_impressions'] && task['viewability_measured_rate_index'] && task['impressions_index']
      end

      def self.viewable_impressions?(task)
        task['viewable_impressions'] && task['viewability_rate_index'] &&
        task['viewability_measured_rate_index'] && task['impressions_index']
      end

      def init
        @task = task
      end

      def run
        convert_options = {timezone: @task['report_parameters']['timezone']}
        viewability_measured_rate_index = @task['viewability_measured_rate_index']
        viewability_rate_index = @task['viewability_rate_index']
        impressions_index = @task['impressions_index']

        CSV.foreach(@task['tempfile_path'], {headers: true}).each do |row|
          row = Lkqd.try_convert(row, convert_options)
          if Lkqd.measurable_impressions?(@task)
            row << row[impressions_index] * row[viewability_measured_rate_index]
          end

          if Lkqd.viewable_impressions?(@task)
            row << row[impressions_index] * row[viewability_measured_rate_index] * row[viewability_rate_index]
          end
          page_builder.add(row)
        end
        page_builder.finish
        FileUtils.rm_rf(@task['tempfile_path'])

        task_report = {}
        return task_report
      end

      DEFAULT_COLUMNS = {
        # dimensions' columns
        'Time'=> { 'type' => 'timestamp', 'format' => "%Y-%m-%dT%H" },
        'Account'=> { 'type' => 'string' },
        'Supply Source ID'=> { 'type' => 'string' },
        'Supply Source'=> { 'type' => 'string' },
        'Supply Partner'=> { 'type' => 'string' },
        'Environment'=> { 'type' => 'string' },
        'Domain'=> { 'type' => 'string' },
        'App Name'=> { 'type' => 'string' },
        'Bundle ID'=> { 'type' => 'string' },
        'Supply Tag Type'=> { 'type' => 'string' },
        'Demand Partner'=> { 'type' => 'string' },
        'Demand Deal ID' => { 'type' => 'string' },
        'Demand Deal'=> { 'type' => 'string' },
        'Demand Tag'=> { 'type' => 'string' },
        'Format'=> { 'type' => 'string' },
        'Country'=> { 'type' => 'string' },
        'Device Type'=> { 'type' => 'string' },
        'OS'=> { 'type' => 'string' },
        'Width X Height'=> { 'type' => 'string' },
        # "Opportunity report" columns
        'Tag Loads' => { 'type' => 'long' },
        'Opportunities' => { 'type' => 'long' },
        'Format Loads' => { 'type' => 'long' },
        'Format Fill Rate' => { 'type' => 'double' },
        'Ineligible Ops: Demand' => { 'type' => 'long' },
        'Ineligible Ops: Restrictions' => { 'type' => 'long' },
        'Impressions' => { 'type' => 'long' },
        'Fill Rate' => { 'type' => 'double' },
        'Efficiency Rate' => { 'type' => 'double' },
        'CPM' => { 'type' => 'double' },
        'Revenue' => { 'type' => 'double' },
        'Cost' => { 'type' => 'double' },
        'Profit' => { 'type' => 'double' },
        'Profit Margin' => { 'type' => 'double' },
        'Clicks' => { 'type' => 'long' },
        'CTR' => { 'type' => 'double' },
        'Ad Starts' => { 'type' => 'long' },
        'Ad Starts Rate' => { 'type' => 'double' },
        '25% Views' => { 'type' => 'long' },
        '50% Views' => { 'type' => 'long' },
        '75% Views' => { 'type' => 'long' },
        '100% Views' => { 'type' => 'long' },
        '25% View Rate' => { 'type' => 'double' },
        '50% View Rate' => { 'type' => 'double' },
        '75% View Rate' => { 'type' => 'double' },
        '100% View Rate' => { 'type' => 'double' },
        'Viewability Measured Rate' => { 'type' => 'double' },
        'Viewability Rate' => { 'type' => 'double' },
        # "Request report" columns
        'Tag Requests' => { 'type' => 'long' },
        'Ads' => { 'type' => 'long' },
        'VAST Ads' => { 'type' => 'long' },
        'VPAID Ads' => { 'type' => 'long' },
        'Wins' => { 'type' => 'long' },
        'Ad Rate' => { 'type' => 'double' },
        'VAST Ad Rate' => { 'type' => 'double' },
        'VPAID Ad Rate' => { 'type' => 'double' },
        'Win Rate' => { 'type' => 'double' },
        'VPAID Responses' => { 'type' => 'long' },
        'VPAID Attempts' => { 'type' => 'long' },
        'VPAID Successes' => { 'type' => 'long' },
        'VPAID Opt Outs' => { 'type' => 'long' },
        'VPAID Timeouts' => { 'type' => 'long' },
        'VPAID Errors' => { 'type' => 'long' },
        'VPAID Success Rate' => { 'type' => 'double' },
        'VPAID Opt Out Rate' => { 'type' => 'double' },
        'VPAID Timeout Rate' => { 'type' => 'double' },
        'VPAID Error Rate' => { 'type' => 'double' },
        'Tag Timeouts' => { 'type' => 'long' },
        'Tag Timeout Rate' => { 'type' => 'double' },
        'Tag Errors' => { 'type' => 'long' },
        'Tag Error Rate' => { 'type' => 'long' },
        'Playback Errors' => { 'type' => 'long' },
        'Playback Error Rate' => { 'type' => 'double' },
        # custom data columns
        'Custom 1'=> { 'type' => 'string' },
        'Custom 2'=> { 'type' => 'string' },
        'Custom 3'=> { 'type' => 'string' },
        'Custom 4'=> { 'type' => 'string' },
        'Custom 5'=> { 'type' => 'string' },
        'Custom 6'=> { 'type' => 'string' },
        'Custom 7'=> { 'type' => 'string' },
        'Custom 8'=> { 'type' => 'string' },
        'Custom 9'=> { 'type' => 'string' },
        'Custom 10'=> { 'type' => 'string' },
      }.freeze
    end

  end
end

