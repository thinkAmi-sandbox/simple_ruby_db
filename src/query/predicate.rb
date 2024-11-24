class Predicate
  attr_accessor :terms

  def initialize(term: nil)
    @terms = []
    unless term.nil?
      @terms.push(term)
    end
  end

  def conjunction_with(predicate)
    terms.concat(predicate.terms)
  end

  def satisfied?(scan)
    terms.all? { |term| term.satisfied?(scan) }
  end

  def reduction_factor(plan)
    terms.reduce(1) { |total, term| (total * term.reduction_factor(plan)).to_i }
  end

  def select_sub_predicate(schema)
    result = Predicate.new

    filtered_terms = terms.filter {|t| term.applies_to?(schema) }
    return nil if filtered_terms.length == 0

    result.terms.concat(filtered_terms)
    result
  end

  def join_sub_predicate(schema1, schema2)
    result = Predicate.new
    new_schema = Schema.new

    new_schema.add_all(schema1)
    new_schema.add_all(schema2)

    filtered_terms = terms.filter {|t| !term.applies_to?(schema1) && !term.applies_to?(schema2) && term.applies_to?(new_schema) }
    return nil if filtered_terms.length == 0

    result.terms.concat(filtered_terms)
    result
  end

  def equals_with_constant(field_name)
    terms.find { |term| !term.equals_with_constant(field_name).nil? }
  end

  def equals_with_field(field_name)
    terms.find { |term| !term.equals_with_field(field_name).nil? }
  end

  def to_s
    terms.reduce('') do |result, term|
      next term.to_s  if result.length == 0
      "#{result} and #{term.to_s}"
    end
  end
end